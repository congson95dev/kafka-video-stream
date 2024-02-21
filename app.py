import cv2
import numpy as np
from flask import Flask, Response, render_template
from kafka import KafkaConsumer

app = Flask(__name__)

# this file got idea by this github repo and chatgpt
# https://github1s.com/pborgesEdgeX/Video-RTSP-Streaming-/blob/master/consumer_localhost/main.py

consumer = KafkaConsumer(
    'KafkaVideoStream',
    bootstrap_servers='localhost:29092',
    fetch_max_bytes=52428800,
    fetch_max_wait_ms=1000,
    fetch_min_bytes=1,
    max_partition_fetch_bytes=1048576,
    value_deserializer=None,
    key_deserializer=None,
    max_in_flight_requests_per_connection=100,
    client_id='KafkaVSClient',
    group_id='KafkaVideoStreamConsumer',
    auto_offset_reset='earliest',
    max_poll_records=500,
    max_poll_interval_ms=300000,
    heartbeat_interval_ms=3000,
    session_timeout_ms=10000,
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    reconnect_backoff_ms=50,
    reconnect_backoff_max_ms=500,
    request_timeout_ms=305000,
    receive_buffer_bytes=32768,
)


@app.route('/')
def index():
    return render_template('index.html')


def generate_frames():
    count = 0
    while True:
        # fetch each frame of the video
        payload = consumer.poll(500)
        for bucket in payload:
            for msg in payload[bucket]:
                msg_val = msg.value
                nparr = np.frombuffer(msg_val, np.uint8)
                frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                ret, buffer = cv2.imencode('.jpg', frame)
                frame = buffer.tobytes()
                count += 1
                print('Count consume: {} time.'.format(count))
                # yield for each frame of the video and show video to screen
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')


@app.route('/video_feed')
def video_feed():
    return Response(generate_frames(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


if __name__ == '__main__':
    app.run(debug=True)
