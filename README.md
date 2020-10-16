# fm-analytics

## Setup

Install pip-tools:

```
pip install pip-tools
```

Install dependencies:

```
pip install -r requirements.txt
```

Start proxy service:

```
docker-compose up -d proxy && docker-compose logs -f
```