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

# Relaxed yt-dlp options with mobile client emulation as final fallback
ydl_opts = {
    'format': 'bestaudio/best/ba/b',
    'quiet': True,
    'no_warnings': True,
    'extract_flat': False,
    'socket_timeout': 10,
    'cookiefile': 'cookies.txt',  # Exact Python equivalent of --cookies cookies.txt
}

# Public Invidious and Piped mirrors to bypass datacenter IP bot walls
STREAM_APIS = [
    {"type": "invidious", "url": "https://inv.tux.pizza/api/v1/videos/"},
    {"type": "invidious", "url": "https://invidious.nerdvpn.de/api/v1/videos/"},
    {"type": "invidious", "url": "https://invidious.jing.rocks/api/v1/videos/"},
    {"type": "piped", "url": "https://pipedapi.kavin.rocks/streams/"},
    {"type": "piped", "url": "https://api.piped.privacydev.net/streams/"}
]

room_state = {
    "queue": [],
    "current_track": None,
    "stream_url": None,
    "is_playing": False,
    "current_time": 0
}

def resolve_audio_url(video_id):
    """Attempts stream resolution across public mirrors before falling back to yt-dlp."""
    for api in STREAM_APIS:
        try:
            target = f"{api['url']}{video_id}"
            resp = requests.get(target, timeout=5)
            if resp.status_code != 200:
                continue

            data = resp.json()

            if api['type'] == 'invidious':
                formats = data.get('adaptiveFormats', [])
                audio_streams = [f for f in formats if f.get('type', '').startswith('audio/')]
                if audio_streams:
                    best = sorted(audio_streams, key=lambda x: int(x.get('bitrate', 0)), reverse=True)[0]
                    stream_url = best.get('url')
                    if stream_url:
                        print(f"[SUCCESS] Resolved via Invidious mirror: {api['url']}")
                        return stream_url

            elif api['type'] == 'piped':
                audio_streams = data.get('audioStreams', [])
                if audio_streams:
                    best = sorted(audio_streams, key=lambda x: x.get('bitrate', 0), reverse=True)[0]
                    stream_url = best.get('url')
                    if stream_url:
                        print(f"[SUCCESS] Resolved via Piped mirror: {api['url']}")
                        return stream_url

        except Exception as err:
            print(f"[DEBUG] Proxy mirror {api['url']} unreachable: {err}")
            continue

    # Fallback to local yt-dlp if proxy mirrors fail
    try:
        print("[DEBUG] Falling back to direct yt-dlp extraction...")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            url = info.get('url')
            if url:
                return url
    except Exception as e:
        print(f"[ERROR] yt-dlp fallback failed: {e}")

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
