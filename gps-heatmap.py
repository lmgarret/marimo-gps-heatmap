# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.4.1",
#     "geopandas==1.0.1",
#     "gpxpy==1.6.2",
#     "influxdb-client==1.46.0",
#     "leafmap==0.38.5",
#     "maplibre==0.2.6",
#     "marimo",
#     "pandas==2.2.3",
#     "python-dateutil==2.9.0.post0",
#     "seaborn==0.13.2",
#     "shapely==2.0.6",
# ]
# ///

import marimo

__generated_with = "0.9.10"
app = marimo.App(width="medium", layout_file="layouts/gps-heatmap.grid.json")


@app.cell(hide_code=True)
def __():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        """
        # Nextcloud + HomeAssistant based GPS Heatmap
        In this notebook, we load GPS data points from multiple data sources, and derive a heatmap out of it. The time window for the heatmap can be configured with a slider, and some data tables can be tweaked using marimo UI select boxes.

        We use data coming from:
         - InfluxDB, as stored by HomeAssistant when tracking a person's location
         - GPX files, originally coming from the Android FOSS app PhoneTrack and pushed to Nextcloud maps
        Both data sources require some data cleaning and alignement, we'll get back to it later.

        Finally, we build a single DataFrame using all these sources, and derive a weight for each data point to ensure that the heatmap is built on the intensity and not the sampling frequency.
        """
    )
    return


@app.cell
def __(mo):
    time_slider = mo.ui.slider(start=1, stop=48, step=1, debounce=True, full_width=True, show_value=True, label="Months to display")
    time_slider
    return (time_slider,)


@app.cell
def __(mo, time_slider):
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import pandas as pd

    start_date = pd.Timestamp('now').floor('D') + pd.offsets.MonthBegin(-time_slider.value)
    mo.md(f"Start date: {start_date}")
    return datetime, pd, relativedelta, start_date


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        """
        ## Loading from InfluxDB
        We load settings to connect to our InfluxDB database. A template file `.conf.json.template` is given in the repository. 

        Our data was stored by the HomeAssistant's InfluxDB integration. To get a list of the persons we can load the GPS history for we can filter our data on the `r.domain == "person"` predicate.
        """
    )
    return


@app.cell
def __(mo, time_slider):
    from influxdb_client import InfluxDBClient
    from conf import GlobalConf

    conf = GlobalConf.load(".conf.json")

    client = InfluxDBClient(**conf["influxdb"])
    query_api = client.query_api()
    ## using Table structure
    tables = query_api.query(f"""
    from(bucket:"hass/autogen")
        |> range(start: -{time_slider.value}mo)
        |> filter(fn: (r) => r.domain == "person")
        |> group(columns: ["entity_id"])
        |> count()
    """)

    table_names = [table.records[0]["entity_id"] for table in tables]
    selected_table = mo.ui.dropdown(
        label="Select the person to load GPS points for", options=table_names, value=table_names[0]  # default to 1st person
    )
    selected_table
    return (
        GlobalConf,
        InfluxDBClient,
        client,
        conf,
        query_api,
        selected_table,
        table_names,
        tables,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        We can now load our data points, querying only the ones within our time window.

        > More context
        > 
        > In a perfect world, we would only have a single table to query from. Unfortunately this is the real world were some data cleaning is needed. In my case, I have a few tables that are not aligned: I had to migrate at some point from InfluxDB v1 to v2, tweaking the HomeAssistant configuration along the way. This resulted in a few metadata columns being changed in the middle of the recorded series:
        > 
        > - new tables add a `"source": "HA"` column
        > - new tables include a `"friendly_name"` column
        > - the `"_measurement"` column have different values between the v1 setup and the v2 one
        >
        >
        > To align these, we add a `|> drop()` operation to our query to remove these columns we don't need anyway. If we don't, we would end up with multiple DataFrames as outputs that we would need to manually merge. Better use InfluxDB's capabilities to return us a single and clean DataFrame!
        """
    )
    return


@app.cell
def __(query_api, selected_table, time_slider):
    influx_points = query_api.query_data_frame(f"""
    from(bucket:"hass/autogen")
        |> range(start: -{time_slider.value}mo)
        |> filter(fn: (r) => r.domain == "person" and r.entity_id == "{selected_table.value}" and (r._field == "latitude" or r._field == "longitude"))
        |> drop(columns: ["_measurement", "source", "friendly_name"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """)  # dropping the columns is essential to align the data and make sure that a single table is returned

    influx_points = influx_points.drop(columns=["result", "table", "_start", "_stop", "domain", "entity_id"]).rename(columns={"_time": "time"})

    influx_points
    return (influx_points,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        """
        ## Loading from GPX files
        We now load points coming from GPX files. Initially, when I started keeping track of my location, I was making use of the FOSS Android app PhoneTrack, with a job pushing to Nextcloud the device's location. I now only rely on HomeAssistant + InfluxDB since I need the location anyway for some automation tasks.

        The GPX files can be put in the `gpx/` directory, and the next cell allows to select which one to use or use all of them.

        > More context
        > 
        > I started recording with PhoneTrack since 2020-11, but downloading the .GPX file from Nextcloud maps for my device shows  only records up until 2024-01 (~9 months when this notebook is being written). It's possible that I did erase some of these records by mistake. But fortunately, I did export a backup .GPX file directly from PhoneTrack that contains records starting from 2020-11, but missing a few more recent months. So that's why I'm loading from multiple GPX files, also making sure to drop duplicate entries
        """
    )
    return


@app.cell
def __(mo):
    import gpxpy
    import os
    import glob

    gpx_files = glob.glob('gpx/*.gpx')

    selected_gpx = mo.ui.dropdown(
        label="Select GPX file", options=["All", *gpx_files], value="All"  # default to all files
    )
    selected_gpx
    return glob, gpx_files, gpxpy, os, selected_gpx


@app.cell
def __(datetime, gpx_files, gpxpy, pd, selected_gpx):
    tmp_gpx_points = []

    selected_gpx_files = gpx_files if selected_gpx.value == "All" else [selected_gpx.value]
    for gpx_file in selected_gpx_files:
        with open(gpx_file) as f:
            gpx = gpxpy.parse(f)

        # Convert to a dataframe one point at a time.
        for track in gpx.tracks:
            for segment in track.segments:
                _offset=0
                for p in segment.points:
                    tmp_gpx_points.append({
                        'time': p.time if p.time else datetime.fromtimestamp(1605006951 + _offset),
                        'latitude': p.latitude,
                        'longitude': p.longitude,
                        #'elevation': p.elevation,
                    })
                    _offset+=1
    gpx_points = pd.DataFrame.from_records(tmp_gpx_points).drop_duplicates(subset=["time"])
    gpx_points["time"] = pd.to_datetime(gpx_points["time"], utc=True)
    gpx_points = gpx_points.sort_values(by="time", ascending=True)
    gpx_points
    return (
        f,
        gpx,
        gpx_file,
        gpx_points,
        p,
        segment,
        selected_gpx_files,
        tmp_gpx_points,
        track,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        """
        ## Merge data sources
        We now merge the data coming from both sources; this requires aligning the tables columns, concatenating them and making sure to drop duplicate points.

        As we want the heatmap to reflect actual intensity rather than just sampling frequency, we assign weights to each data point based on the inverse sampling frequency. A `time_bin` column is used to group timestamps into hourly intervals. The `interval_counts` series then calculates how many points fall into each interval. The weights are then inversely proportional to these counts, ensuring that denser intervals don't overshadow sparser ones just because of a higher sampling rate.
        """
    )
    return


@app.cell
def __(gpx_points, influx_points, pd, start_date):
    # Filter the dataframes based on the time window
    gpx_points_filtered = gpx_points[gpx_points["time"] >= pd.to_datetime(start_date, utc=True)]

    # Combine the dataframes, prioritizing df1
    all_points = pd.concat([influx_points, gpx_points_filtered[~gpx_points_filtered['time'].isin(influx_points["time"])]])
    all_points = all_points.sort_values(by="time", ascending=True)

    # compute weight for each point

    # Calculate the frequency of each timestamp
    # Define the interval (e.g., hourly)
    all_points["time_bin"] = pd.to_datetime(all_points["time"]).dt.floor("h")

    # Calculate the frequency for each interval
    interval_counts = all_points["time_bin"].value_counts().sort_index()


    # Map these counts back to the DataFrame to normalize weights
    all_points["weight"] = all_points["time_bin"].map(lambda x: 1 / interval_counts[x])

    all_points
    return all_points, gpx_points_filtered, interval_counts


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""Let's display some distribution information about our data:""")
    return


@app.cell
def __(all_points, mo):
    import altair as alt

    monthly_counts = all_points.groupby(all_points["time"].dt.to_period("M")).size().reset_index(name="count")
    chart = mo.ui.altair_chart(alt.Chart(monthly_counts).mark_bar().encode(
        x='time',
        y='count',
    ))
    chart
    return alt, chart, monthly_counts


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Rendering on a map
        Finally, we can render our heatmap using [`leafmap`](https://leafmap.org/). At the time of writing this notebook (`v0.38.5`), the default plotting backend (`ipyleaflet`) exposed as the base `leafmap` module unfortunately suffered bugs in `marimo`:

         - the heatmap was initially correctly rendered but moving the map/toggling the heatmap/zooming in and out will completely change it
         - the map UI freezed after a few map manipulations; one has to restart the cell

        Fortunately, `leafmap` provides a few other plotting backends, and using [`maplibre`](https://maplibre.org/) worked as a charm!
        """
    )
    return


@app.cell
def __():
    # in this cell, we take an existing seaborn palette and convert it to a palette expected by maplibre

    import seaborn as sns

    # Generate a palette
    palette =  sns.color_palette("rocket", 5)

    # Convert the palette to CSS-compatible format
    css_colors = [f"rgb({int(r*255)},{int(g*255)},{int(b*255)})" for r, g, b in palette]
    css_colors[0] = css_colors[0].replace(")", ",0)")

    # Create the heatmap-color expression for MapLibre
    heatmap_color = [
        "interpolate",
        ["linear"],
        ["heatmap-density"]
    ]

    # Adding stops to the heatmap-color expression
    for i, color in enumerate(css_colors):
        stop_value = i / (len(css_colors) - 1)
        heatmap_color.extend([stop_value, color])
    return color, css_colors, heatmap_color, i, palette, sns, stop_value


@app.cell
def __(all_points, heatmap_color, mo):
    # in this cell, we instantiate the Map and add our points as source

    import geopandas as gpd
    import leafmap.maplibregl as leafmap
    from shapely.geometry import Point

    # convert to a geopandas DataFrame
    all_points_gdf = gpd.GeoDataFrame(all_points["weight"], geometry=[Point(xy) for xy in zip(all_points['longitude'], all_points['latitude'])])

    mean_x = all_points_gdf.geometry.x.mean()
    mean_y = all_points_gdf.geometry.y.mean()

    # extremas to fit bounds
    south_west_point = all_points_gdf.geometry.bounds[['minx', 'miny']].min().values.tolist()
    north_east_point = all_points_gdf.geometry.bounds[['maxx', 'maxy']].max().values.tolist()

    m = leafmap.Map(center=[mean_x, mean_y], zoom=1, height="600px", style="positron")
    m.fit_bounds([south_west_point, north_east_point])
    source = {
        "type": "geojson",
        "data": all_points_gdf.to_geo_dict(),
        # "data": "https://maplibre.org/maplibre-gl-js/docs/assets/earthquakes.geojson",
    }

    m.add_source("gps-points", source)
    layer = {
        "id": "gps-heatmap",
        "type": "heatmap",
        "source": "gps-points",
        "paint": {
            # Increase the heatmap weight based on frequency and property weight
            # "heatmap-weight": ["get", "weight"],
            "heatmap-weight": ["interpolate", ["linear"], ["get", "weight"], 0, 0, 1, 0.1],
            # # Increase the heatmap color weight weight by zoom level
            # # heatmap-intensity is a multiplier on top of heatmap-weight
            "heatmap-intensity": ["interpolate", ["linear"], ["zoom"], 0, 0.1, 20, 10],
            "heatmap-color": heatmap_color,
            # Adjust the heatmap radius by zoom level
            "heatmap-radius": ["interpolate", ["linear"], ["zoom"], 0, 10, 20, 20],
            # Transition from heatmap to circle layer by zoom level
            "heatmap-opacity": ["interpolate", ["linear"], ["zoom"], 7, 0.8, 20, 0.2],
        },
    }
    m.add_layer(layer)
    mo.iframe(m.to_html())  # trick to force reloading on cell execution
    return (
        Point,
        all_points_gdf,
        gpd,
        layer,
        leafmap,
        m,
        mean_x,
        mean_y,
        north_east_point,
        source,
        south_west_point,
    )


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
