## Training routine

The toy linear-regression example lives in `src/train.py`. Run it with uv so
dependencies come from the managed environment:

```bash
uv run python src/train.py
```

It will train on `data.csv`, print validation metrics, and can be imported for
`train_model`/`validate_model` helpers inside other scripts.
