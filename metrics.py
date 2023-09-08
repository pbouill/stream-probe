from pathlib import Path
import pandas as pd

BASE_PATH = Path(__file__).parent

TIMESTAMP_COLUMN = 'timestamp'
PERIOD_COLUMN = 'period'

def get_data(path=BASE_PATH):
    df = None
    for f in path.glob('*.csv'):
        data = pd.read_csv(f, header='infer', comment='#')
        # print(data.head())
        data[TIMESTAMP_COLUMN] = pd.to_datetime(data[TIMESTAMP_COLUMN], format='ISO8601')
        data.set_index(TIMESTAMP_COLUMN, inplace=True)
        data[PERIOD_COLUMN] = pd.to_timedelta(data[PERIOD_COLUMN])
        if df is None:
            df = data
        else:
            df = pd.concat([df, data])
    print(df.info())
    return df

def get_fps(df: pd.DataFrame):
    s = (1/df[PERIOD_COLUMN].dt.total_seconds())
    s.name = 'fps'
    return s


if __name__ == '__main__':
    LOG_PATH = BASE_PATH.joinpath('log')
    df = get_data(path=LOG_PATH)
    fps = get_fps(df=df)

    plot = fps.plot(kind='kde')
    fig = plot.get_figure()
    fig.savefig(LOG_PATH.joinpath('density.png'))

    plot = fps.to_frame().reset_index().plot(title='FPS', kind='scatter', x=fps.index.name, y=fps.name, s=1, use_index=True)
    fig = plot.get_figure()
    fig.savefig(LOG_PATH.joinpath('scatter.png'))

    print(f'Standard Deviation: {fps.std()}')
    print(f'Median: {fps.median()}')
    print(f'Mean: {fps.mean()}')
    print(f'Minimum: {fps.min()}')
    print(f'Maximum: {fps.max()}')
    
    plot = fps.plot(kind='kde')
    # plot = fps.reset_index().plot(title='1200L Collar FPS', kind='scatter', x=TIMESTAMP_COLUMN, y='fps')

    print(f'Standard Deviation: {fps.std()}')
    print(f'Median: {fps.median()}')
    print(f'Mean: {fps.mean()}')
    print(f'Minimum: {fps.min()}')
    print(f'Maximum: {fps.max()}')
