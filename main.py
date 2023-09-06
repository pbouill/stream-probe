import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from dataclasses import dataclass, asdict
import csv
import cv2
import yaml

DATA_LOG_DIR = Path(os.environ.get('DATA_LOG_DIR', './log'))
CONFIG_FILE = Path(os.environ.get('CONFIG_FILE', 'config.yaml'))

LOG_FORMAT = (
    '%(asctime)s.%(msecs)06d :: %(levelname)s :: %(name)s :: %(module)s.%(funcName)s:%(lineno)d - %(message)s'
)
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)

@dataclass
class RowData:
    timestamp: datetime
    frames: int = None
    dropped: int = None
    success: bool = None
    period: timedelta = None
    fps: float = None

    @property
    def row_list(self):
        return [self.timestamp, self.frames, self.dropped, self.success, self.period, self.fps]


def read_config(config_file: Path = CONFIG_FILE):
    with config_file.open() as f:
        config = yaml.safe_load(f)
    return config

def get_stream_uri(url: str, username: str = None, password: str = None):
    url = urlparse(url=url)
    netloc = url.netloc.split('@')[-1]
    if username is not None:
        cred_str = username
        if password is not None:
            cred_str += ':' + password
        cred_str += '@'
    else:
        cred_str = ''
    return url._replace(netloc=f'{cred_str}{netloc}').geturl()

def run(url: str, username: str = None, password: str = None):
    uri = get_stream_uri(url=url, username=username, password=password)
    capture = cv2.VideoCapture(uri)
    count = 0
    dropped = 0
    last_success_ts = None
    max_period = None
    max_fps = None
    DATA_LOG_DIR.mkdir(exist_ok=True)

    # csv_log = csv.writer()

    while capture.isOpened():
        ts = datetime.utcnow()
        curr_log_hr = ts.hour
        csv_log_f = DATA_LOG_DIR.joinpath(f'log_{ts.strftime("%Y%m%d_%H.csv")}')
        logger.info(f'creating a new csv log file: {csv_log_f}')
        with csv_log_f.open('w+') as csv_file:
            writer = csv.writer(csv_file)
            while ts.hour == curr_log_hr:
                ts = datetime.utcnow()
                has_frame, frame = capture.read()
                period = None
                if has_frame:
                    count += 1
                    if last_success_ts is not None:
                        period = ts - last_success_ts
                    if max_period is not None:
                        if period > max_period:
                            max_period = period
                            logger.info(f'longest frame capture period detected: {max_period}')
                    else:
                        max_period = period
                    last_success_ts = ts
                else:
                    dropped += 1

                fps = capture.get(cv2.CAP_PROP_FPS)
                if max_fps is not None:
                    if fps > max_fps:
                        max_fps = fps
                        logger.info(f'gratest fps value observed: {max_fps}')

                row = RowData(timestamp=ts, frames=count, dropped=dropped, success=has_frame, period=period, fps=fps)
                writer.writerow(row.row_list)



if __name__ == '__main__':
    config = read_config()
    logger.debug(f'Configuration is: {config}')

    # extract the stream connection details
    stream_config = config.get('stream')
    stream_url = stream_config.get('url')
    stream_username = stream_config.get('username', None)
    stream_password = stream_config.get('password', None)

    run(url=stream_url, username=stream_username, password=stream_password)

    # uri = get_stream_uri(url=stream_url, username=stream_username, password=stream_password)
    # logger.info(f'stream uri is: {uri}')
    
    # capture = cv2.VideoCapture(uri)
    
    


