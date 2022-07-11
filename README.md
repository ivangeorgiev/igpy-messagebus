# igpy-messagebus
Python  Message Bus

## Build

```bash
$ pip install build
$ python -m build
```

## Testing

Unit tests are using `pytest`. To execute the unit tests:

```bash
$ pytest
..........
```

For functional tests, python `behave` is used. To execute functional tests:
```
$ behave tests\features
```


## Publish

```bash
$ pip install -U twine
$ python -m twine upload dist/*
Enter your username: ******
Enter your password: ******
```
