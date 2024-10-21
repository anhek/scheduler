## Scheduler
Simple scheduler for assigning jobs to compute nodes

### Setup

```shell
python3 -m venv venv
source venv/bin/activate
pip install .
```

### Run

```shell
python src/webserver.py
```

### Build dist

```shell
python -m build
```

### Test setup
```shell
pip install .[test]
```

### Test run
```shell
pytest
```
