# kafka-video-stream

Install zookeeper and kafka by docker
```
docker compose up -d --build
```

Install environment
```
cd project_name

python3 -m venv venv

source venv/bin/activate

pip install -r requirements.txt
```

# Setup and start producer

```
python3 producer.py video.mp4
```

There's 2 types of the results in this project:
1. show video in **terminal** using `consumer.py`
2. show video in **browser** using `app.py`

## Setup and start consumer

```
python3 consumer.py
```

Video should start playing in your screen. To stop, simply presh button '**Ctrl** + **C**'.

# To run on browser:

```
python3 app.py
```

Go to `127.0.0.1:5000` to see the video is running.

# Note:

The logic inside is:

1. the producer go through each frame of the video and upload it to the Kafka Topic

2. the consumer will consume the message through Kafka Topic and fetch each frame of the video and render to the screen.

