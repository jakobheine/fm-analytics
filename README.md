# fm-analytics

## Setup

Activate venv:

```
source venv/bin/activate
```

Install pip-tools:

```
pip install pip-tools
```

Create requirements.txt from requirements.in:

```
pip-compile requirements.in
```

Install dependencies:

```
pip install -r requirements.txt
```

Start proxy service:

```
docker-compose up -d proxy && docker-compose logs -f
```