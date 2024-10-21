import os
import logging

from pynwb import NWBHDF5IO
from pynwb.ecephys import ElectricalSeries

from config import Config
from importer import import_timeseries
from writer import TimeSeriesChunkWriter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger()

if __name__ == "__main__":
    config = Config()

    bytes_per_mb = pow(2, 20)
    bytes_per_sample = 8 # 64-bit floating point value
    chunk_size = int(config.CHUNK_SIZE_MB * bytes_per_mb / bytes_per_sample)

    log.info(f"INPUT_DIR={config.INPUT_DIR} | OUTPUT_DIR={config.OUTPUT_DIR}")

    input_files = [
        f.path
        for f in os.scandir(config.INPUT_DIR)
        if f.is_file() and os.path.splitext(f.name)[1].lower() == '.nwb'
    ]

    for file in input_files:
        log.info(f"INPUT FILE: {file}")

    assert len(input_files) == 1, "NWB post processor only supports a single file as input"

    with NWBHDF5IO(input_files[0], mode="r") as io:
        nwb = io.read()
        electrical_series = [acq for acq in nwb.acquisition.values() if type(acq) == ElectricalSeries]
        if len(electrical_series) < 1:
            log.error('NWB file has no continuous raw electrical series data')
        if len(electrical_series) > 1:
            log.warn('NWB file has multiple raw electrical series acquisitions')

        chunked_writer = TimeSeriesChunkWriter(nwb.session_start_time, config.OUTPUT_DIR, chunk_size)

        for series in electrical_series:
            chunked_writer.write_electrical_series(series)

    # import requires Pennsieve API access; when developing locally this is most often not required
    # note: this will be moved to a separated post-processor once the analysis pipeline is more
    # easily able to handle > 3 processors
    if config.IMPORTER_ENABLED:
        importer = import_timeseries(config.API_HOST, config.API_HOST2, config.API_KEY, config.API_SECRET, config.WORKFLOW_INSTANCE_ID, config.OUTPUT_DIR)
