import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_metrics(df, *args, **kwargs):
    df.to_csv('iris_metrics.csv', index=False)
    print('Saved metrics to iris_metrics.csv')
    return df


@test
def test_metrics_table(output, *args):
    assert len(output) == 4, 'expected 4 metric rows'
    assert list(output.columns) == ['metric', 'value'], 'unexpected columns'
    assert output['value'].between(0, 1).all(), 'metric values must be between 0 and 1'