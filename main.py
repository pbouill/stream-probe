import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from dataclasses import dataclass, asdict, fields
import csv
import cv2
import yaml
import numpy as np

DATA_LOG_DIR = Path(os.environ.get('DATA_LOG_DIR', './log'))
CONFIG_FILE = Path(os.environ.get('CONFIG_FILE', 'config.yaml'))
FPS = os.environ.get('FPS', None)

LOG_FORMAT = (
    '%(asctime)s.%(msecs)06d :: %(levelname)s :: %(name)s :: %(module)s.%(funcName)s:%(lineno)d - %(message)s'
)
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)

@dataclass
class RowData:
    timestamp: datetime
    frames: int = None
    unique_frames: int = None
    dropped: int = None
    success: bool = None
    period: timedelta = None

    @property
    def row_list(self):
        return asdict(self).values()
    
    @classmethod
    def get_row_header(cls):
        return [f.name for f in fields(cls)]


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

def run(url: str, username: str = None, password: str = None, fps: int = None):
    uri = get_stream_uri(url=url, username=username, password=password)
    capture = cv2.VideoCapture(uri)
    frames = 0
    unique_frames = 0
    dropped = 0
    success_period = None
    last_success_ts = None
    last_frame = None
    last_unique_frame_ts = None
    max_period = None
    max_unique_period = None
    
    stream_fps = capture.get(cv2.CAP_PROP_FPS)  # get the actual fps for the stream...
    logger.info(f'FPS for the stream is: {stream_fps}')
    if fps is None:
        fps = stream_fps 
    fps = int(fps)
    spf = 1/fps  # get the proper seconds per frame
    logger.info(f'Will attempt to return frames at: {fps} FPS ({spf} sec/frame)')

    DATA_LOG_DIR.mkdir(exist_ok=True)

    while capture.isOpened():
        ts = datetime.utcnow()
        curr_log_hr = ts.hour
        csv_log_f = DATA_LOG_DIR.joinpath(f'log_{ts.strftime("%Y%m%d_%H.csv")}')
        logger.info(f'creating a new csv log file: {csv_log_f}')
        write_header = not csv_log_f.exists()
        with csv_log_f.open('a') as csv_file:
            writer = csv.writer(csv_file)  # the writer object for our csv file
            if write_header:
                csv_file.write(f'# URL: {url}\n')
                csv_file.write(f'# FPS: {fps}\n')
                writer.writerow(RowData.get_row_header())
            while ts.hour == curr_log_hr:  # if the hour rolls over, will need to create a new csv file (outer while loop)...
                ts = datetime.utcnow()
                if (last_success_ts is not None) and (last_unique_frame_ts is not None):        
                    success_period = ts - last_success_ts
                    unique_period = ts - last_unique_frame_ts
                    if (success_period.total_seconds() < spf):
                        continue  # no sense trying to capture faster than the FPS

                has_frame, frame = capture.read()

                if has_frame:
                    frames += 1
                    if last_frame is not None:
                        if not np.array_equal(frame, last_frame):
                            unique_frames += 1
                            if last_unique_frame_ts is not None:
                                if max_unique_period is not None:
                                    if unique_period > max_unique_period:
                                        max_unique_period = unique_period
                                        logger.info(f'longest time between unique frames detected: {max_unique_period}')
                                else:
                                    max_unique_period = unique_period
                            last_unique_frame_ts = ts
                        else:
                            logger.warning(f'returned a non-unique/duplicate frame from previous pass!')
                    else:
                        unique_frames += 1
                        last_unique_frame_ts = ts
                    
                    if last_success_ts is not None:
                        success_period = (ts - last_success_ts)
                        if max_period is not None:
                            if success_period > max_period:
                                max_period = success_period
                                logger.info(f'longest frame capture period detected: {max_period}')
                        else:
                            max_period = success_period
                    last_success_ts = ts
                else:
                    dropped += 1

                row = RowData(timestamp=ts, frames=frames, unique_frames=unique_frames, dropped=dropped, success=has_frame, period=success_period)  # create our row data to write...
                writer.writerow(row.row_list)  # write to our log csv
                last_frame = frame  # update our frame memory...


if __name__ == '__main__':
    config = read_config()
    logger.debug(f'Configuration is: {config}')

    # extract the stream connection details
    stream_config = config.get('stream')
    stream_url = stream_config.get('url')
    stream_username = stream_config.get('username', None)
    stream_password = stream_config.get('password', None)

    fps = config.get('fps', FPS)

    run(url=stream_url, username=stream_username, password=stream_password, fps=fps)
    
    


