import csv
import os
import time
from datetime import datetime, timezone
from opensky_api import OpenSkyApi

OUTPUT_FILE = "opensky_live_data.csv"
INTERVAL_SECONDS = 10

FIELDS = [
    "snapshot_time_utc",
    "time_position",
    "icao24",
    "callsign",
    "latitude",
    "longitude",
    "baro_altitude",
    "velocity",
    "true_track",
    "vertical_rate",
    "on_ground",
]

api = OpenSkyApi()

try:
    file_exists = (
        os.path.exists(OUTPUT_FILE)
        and os.path.getsize(OUTPUT_FILE) > 0
    )

    with open(
        OUTPUT_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(file, fieldnames=FIELDS)

        if not file_exists:
            writer.writeheader()

        print("Collecting OpenSky observations.")
        print("Press Ctrl+C to stop.")

        while True:
            try:
                states = api.get_states()

                if states is None or states.states is None:
                    print("No state vectors returned.")
                else:
                    snapshot_time = datetime.now(
                        timezone.utc
                    ).isoformat()

                    count = 0

                    for aircraft in states.states:
                        if (
                            aircraft.latitude is None
                            or aircraft.longitude is None
                        ):
                            continue

                        writer.writerow({
                            "snapshot_time_utc": snapshot_time,
                            "time_position": aircraft.time_position,
                            "icao24": aircraft.icao24,
                            "callsign": aircraft.callsign,
                            "latitude": aircraft.latitude,
                            "longitude": aircraft.longitude,
                            "baro_altitude": aircraft.baro_altitude,
                            "velocity": aircraft.velocity,
                            "true_track": aircraft.true_track,
                            "vertical_rate": aircraft.vertical_rate,
                            "on_ground": aircraft.on_ground,
                        })

                        count += 1

                    file.flush()

                    print(
                        f"{snapshot_time}: "
                        f"saved {count} aircraft observations"
                    )

            except Exception as error:
                print("Request failed:", error)

            time.sleep(INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\nCollection stopped.")

finally:
    api.close()