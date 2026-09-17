import os
import requests
import traceback
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from ytmusicapi import YTMusic
import yt_dlp

app = Flask(__name__)
app.config['SECRET_KEY'] = 'sync_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

ytmusic = YTMusic()

# Public Cobalt API instances for audio extraction
COBALT_INSTANCES = [
    "https://api.cobalt.tools",
    "https://cobalt-api.kwiatekm.tokyo",
    "https://api.wuk.sh"
]

# Fallback yt-dlp configuration
ydl_opts = {
    'format': 'bestaudio/best/ba/b',
    'quiet': True,
    'no_warnings': True,
    'extract_flat': False,
    'socket_timeout': 10,
    'extractor_args': {
        'youtube': {
            'player_client': ['android', 'ios', 'web']
        }
    }
}

room_state = {
    "queue": [],
    "current_track": None,
    "stream_url": None,
    "is_playing": False,
    "current_time": 0
}

def resolve_audio_url(video_id):
    """Fetches direct audio stream links via Cobalt API, falling back to yt-dlp."""
    target_url = f"https://www.youtube.com/watch?v={video_id}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "url": target_url,
        "downloadMode": "audio",
        "audioFormat": "mp3"
    }

    # 1. Primary: Cobalt API mirrors
    for instance in COBALT_INSTANCES:
        try:
            resp = requests.post(instance, json=payload, headers=headers, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                stream_url = data.get('url')
                if stream_url:
                    print(f"[SUCCESS] Audio extracted via Cobalt: {instance}")
                    return stream_url
        except Exception as e:
            print(f"[DEBUG] Cobalt instance {instance} failed: {e}")
            continue

    # 2. Fallback: yt-dlp extraction
    try:
        print("[DEBUG] Falling back to direct yt-dlp...")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(target_url, download=False)
            url = info.get('url')
            if url:
                return url
    except Exception as e:
        print(f"[ERROR] All extractors failed: {e}")

    return None

def fetch_radio_tracks(video_id):
    try:
        watch_playlist = ytmusic.get_watch_playlist(videoId=video_id, limit=5)
        tracks = []
        for item in watch_playlist.get('tracks', [])[1:6]:
            if not item.get('videoId'):
                continue
            tracks.append({
                'id': item.get('videoId'),
                'title': item.get('title'),
                'artist': ', '.join(a['name'] for a in item.get('artists', [])) if item.get('artists') else 'Unknown',
                'thumbnail': item.get('thumbnail', [{}])[-1].get('url', ''),
                'duration': item.get('length', '')
            })
        return tracks
    except Exception as e:
        print(f"[ERROR] Radio fetch failed: {e}")
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/search')
def search():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify([])
    try:
        results = ytmusic.search(query, filter='songs', limit=8)
        if not results:
            results = ytmusic.search(query, limit=8)

        tracks = []
        for item in results:
            vid_id = item.get('videoId')
            if not vid_id:
                continue
            tracks.append({
                'id': vid_id,
                'title': item.get('title', 'Unknown Track'),
                'artist': ', '.join(a['name'] for a in item.get('artists', [])) if item.get('artists') else 'Unknown Artist',
                'thumbnail': item.get('thumbnails', [{}])[-1].get('url', '') if item.get('thumbnails') else '',
                'duration': item.get('duration', '')
            })
        return jsonify(tracks)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@socketio.on('connect')
def handle_connect():
    emit('init_state', room_state)

@socketio.on('sync_event')
def handle_sync(data):
    action = data.get('action')
    
    if action == 'play':
        room_state['is_playing'] = True
        room_state['current_time'] = data.get('time', 0)
    elif action == 'pause':
        room_state['is_playing'] = False
        room_state['current_time'] = data.get('time', 0)
    elif action == 'seek':
        room_state['current_time'] = data.get('time', 0)
    elif action == 'heartbeat':
        room_state['current_time'] = data.get('time', 0)
        emit('drift_sync', {'time': data.get('time', 0)}, broadcast=True, include_self=False)
        return
    elif action == 'add_to_queue':
        track = data.get('track')
        if not track or not track.get('id'):
            return
        room_state['queue'].append(track)
        emit('log_message', {'type': 'success', 'text': f"Added: {track['title']}"}, broadcast=True)

        if not room_state['current_track']:
            play_next_track()
            return
        emit('queue_updated', {'queue': room_state['queue']}, broadcast=True)
        return
    elif action == 'remove_from_queue':
        index = data.get('index')
        if 0 <= index < len(room_state['queue']):
            removed = room_state['queue'].pop(index)
            emit('queue_updated', {'queue': room_state['queue']}, broadcast=True)
        return
    elif action == 'reorder_queue':
        new_queue = data.get('queue')
        if isinstance(new_queue, list):
            room_state['queue'] = new_queue
            emit('queue_updated', {'queue': room_state['queue']}, broadcast=True, include_self=False)
        return

    emit('sync_action', data, broadcast=True, include_self=False)

@socketio.on('next_track')
def handle_next():
    play_next_track()

def play_next_track():
    if len(room_state['queue']) <= 1 and room_state['current_track']:
        recs = fetch_radio_tracks(room_state['current_track']['id'])
        if recs:
            room_state['queue'].extend(recs)
            emit('queue_updated', {'queue': room_state['queue']}, broadcast=True)

    if room_state['queue']:
        track = room_state['queue'].pop(0)
        emit('log_message', {'type': 'info', 'text': f"Resolving stream: {track['title']}..."}, broadcast=True)
        
        stream_url = resolve_audio_url(track['id'])
        if not stream_url:
            emit('log_message', {'type': 'error', 'text': f"Extraction failed for {track['title']}. Skipping."}, broadcast=True)
            play_next_track()
            return

        room_state['current_track'] = track
        room_state['stream_url'] = stream_url
        room_state['is_playing'] = True
        room_state['current_time'] = 0
        
        emit('track_change', {
            'track': room_state['current_track'],
            'stream_url': stream_url,
            'queue': room_state['queue']
        }, broadcast=True)
    else:
        room_state['current_track'] = None
        room_state['stream_url'] = None
        room_state['is_playing'] = False
        emit('queue_ended', broadcast=True)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
