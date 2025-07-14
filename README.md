# copernicusWeather2psdmWeather

Converts [copernicus.eu](https://cds.climate.copernicus.eu/) netCDF4 weather data to [PowerSystemDataModel](https://github.com/ie3-institute/PowerSystemDataModel) weather data format.

## How To Use

### Config File
This tool can be used by giving parameters through config file `config.yaml`.

Here the parameter of the PostGreSql-Database can be assigned:
- `db_user`: Database username, e.g. `postgres`
- `db_password`: Database password of username, e.g. `postgres`
- `db_port`: Database port, e.g. `5432`
- `db_name`: Name of the database, `westfalia_2023_03_24-2023_03_28`
- `input_dir`: Location of the CDS input files, e.g. `input/samples`
- `file_name_base`: First part of the input files, see comment below. e.g. `westfalia_2023_03_24-2023_03_28`

So far we support the following format of input files:
- netCDF4

To start converter
- Adapt config file
- Setup PostGreSql database
- Start `main.py`

### netCDF4 Input files
Climate Data Store (CDS) provides the weather data in two files, which file names ends on '_accum.nc' and '_instant.nc'. Sample data can be found in `input/samples`.

#### File name base
Weather in netCFD format are provided by Copernicus in two files ending with `_accum.nc` and `_instant.nc`.
This parameter allows to set the file_name_base.
E.g. `weather_data_accum.nc` and `weather_data_instant.nc`. file_name_base would be `weather_data`.


## Something Missing? 

We are happy to learn about additional tools for easing the developer workflow. 
Feel free to open an issue or pull-request to make suggestions.