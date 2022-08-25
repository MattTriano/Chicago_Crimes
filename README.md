# Chicago Crime Data Exploration

This repo contains a notebook that explores, visualizes, and analyzes crime data available on the [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). This repo also shows a cleaner way to organize a small, stand-alone data analysis project, with ETL and plotting functionality is defined in separate `.py` files and imported and run in a notebook. This both makes the notebook cleaner and enables other notebooks to import the functionality.

Maps, heatmaps (showing frequency by time-of-day and day-of-week, time-of-day and month, and day-of-week and month), and line plots of monthly and annual rates occurance and arrest for different crimes occuring in Chicago can be viewed in the notebook [`analysis/Chicago_Crime_visualized.ipynb`](https://github.com/MattTriano/Chicago_Crimes/blob/main/analysis/Chicago_Crime_visualized.ipynb). If GitHub can't load this (admittedly large) notebook, you can view it via this [nbViewer link](https://nbviewer.org/github/MattTriano/Chicago_Crimes/blob/main/analysis/Chicago_Crime_visualized.ipynb).

## Running the code locally

If you want to reproduce this on your own machine, clone this repo and enter the cloned repo via the commands below

```bash
(base) user@host: ~/...$ git clone git@github.com:MattTriano/Chicago_Crimes.git
(base) user@host: ~/...$ cd Chicago_Crimes
```

If you don't already have `conda` on your machine, I recommend downloading and installing [miniconda](https://docs.conda.io/en/latest/miniconda.html).

Now that you have conda on your machine, run the commands below 1) to configure `conda` to strictly pull from the conda-forge channel, 2) recreate the `conda env` needed to run the notebook code, 3) make that `conda env` accessible through a jupyter lab/notebook context, and 4) start up a jupyter lab server (which will provide a URL and will probably automatically pass that URL to a browser)

```bash
(base) user@host: ~/.../Chicago_Crimes$ conda config --add channels conda-forge
(base) user@host: ~/.../Chicago_Crimes$ conda config --set channel_priority strict
(base) user@host: ~/.../Chicago_Crimes$ conda env create -f environment.yml
(base) user@host: ~/.../Chicago_Crimes$ conda active geo_env
(geo_env) user@host: ~/.../Chicago_Crimes$ python -m ipykernel install --user --name geo_env --display-name "Python (geo_env)"
(geo_env) user@host: ~/.../Chicago_Crimes$ jupyter lab
```

Open the desired notebook and add parameters `force_repull=True` and `force_remake=True` to `load_clean_chicago_crimes_data()` producing

```python
crimes_gdf = load_clean_chicago_crimes_data(force_repull=True, force_remake=True)
```

update the start and end date to desired dates

```python
nb_start_date="2015-01-01"
nb_end_date="2022-08-18"
```

and run the notebook.