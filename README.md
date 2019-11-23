# Chicago Crime Data Exploration

This repo contains notebooks that explore, visualize, and analyze the crime data available on the [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). The most polished work (as of 11/23/19) is in analysis/Chicag_Crime_Public_v1.ipynb. This notebook is rather large because it contains a large number of maps, plots, and heatmaps, so if it won't load in GitHub, you can view it via this [nbViewer link](https://nbviewer.jupyter.org/github/MattTriano/Chicago_Crimes/blob/master/analysis/Chicag_Crime_Public_v1.ipynb).


If you want to run this on your own machine (and you've independantly downloaded the Crimes data set from the link above), you can use the environment.yml file to automatically create a conda environment with the packages needed to run the code.

To create said conda environment, navigate to the environment.yml file and enter the command

``` conda env create -f environment.yml ```

This environment includes many libraries that aren't needed to run this code, but it's my current build for general geospatial analysis tasks in python.