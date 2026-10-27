from opensky_api import OpenSkyApi

api = OpenSkyApi()

try:
    states = api.get_states()

    if states is None or states.states is None:
        print("No aircraft data returned.")
    else:
        print("Snapshot timestamp:", states.time)
        print("Number of aircraft:", len(states.states))

        for aircraft in states.states[:10]:
            print(
                aircraft.icao24,
                aircraft.callsign,
                aircraft.latitude,
                aircraft.longitude,
                aircraft.baro_altitude,
                aircraft.velocity,
                aircraft.true_track
            )

finally:
    api.close()