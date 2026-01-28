# REFORMERS Digital Twin: Wind Generation Forecasting Model

This repository provides a model for forecasting the power generation of wind turbines for the REFORMERS Digital Twin.
It integrates the [EOLICA] package, relying on wind forecasts from a public weather service to simulate the behaviour of arbitrary wind parks and generate forecasts based on the simulation results.

[EOLICA]: (https://github.com/REFORMERS-EnergyValleys/eolica)

## Overview

[EOLICA] is a stand-alone Python package that was not specifically developed to be deployed via the REFORMERS Digital Twin technical framework.
To integrate it into the REFORMERS Digital Twin technical framework, a simple wrapper has been developed on top of [EOLICA]’s core code:

+ At startup, the tool configuration is read from a local configuration file or generated from information retrieved from the [knowledge graph database](https://github.com/REFORMERS-EnergyValleys/example-reformers-knowledge-graph).
+ Input data (weather forecasts) is read from the data stream, and results (wind power generation forecasts) are written back to the data stream.
+ Simulations are executed periodically using a scheduler.
+ Instructions for containerizing the application ([Dockerfile](./model-src/Dockerfile)) into an executable REFORMERS Digital Twin model are provided.
+ A [generator manifest file](./GENERATOR-MANIFEST.yml) is provided.

## Basic Functionality

### Core Components

1. **EolicaRuntime Class** ([`model-src/eolica-runtime/eolica_runtime_class.py`](./model-src/eolica-runtime/eolica_runtime_class.py)):
   - Manages Redis connections for input/output streams
   - Initializes eolica park and simulation objects from configuration files
   - Runs periodic simulations based on forecast data from Redis
   - Publishes simulation results back to Redis streams

2. **Knowledge Graph Adapter** ([`model-src/eolica-runtime/knowledge_graph_adapter.py`](./model-src/eolica-runtime/knowledge_graph_adapter.py)):
   - Connects to GraphDB via SPARQL queries
   - Retrieves wind park information (roughness, location, etc.)
   - Retrieves turbine information (position, specifications, etc.)
   - Retrieves turbine type definitions (power curves, thrust curves, etc.)

3. **Config Generation** ([`model-src/eolica-runtime/generate_configs_from_graphdb.py`](./model-src/eolica-runtime/generate_configs_from_graphdb.py)):
   - Generates turbine type configuration files from GraphDB
   - Generates park configuration files from GraphDB
   - Supports multiple scenarios and wind park sites

4. **Main Entry Point** ([`model-src/eolica-runtime/__main__.py`](./model-src/eolica-runtime/__main__.py)):
   - Parses configuration file
   - Sets up Redis connection pool
   - Optionally generates configs from GraphDB
   - Initializes and runs the simulation service

### Configuration

The service requires a `config.yml` file with the following structure:

``` YAML
service:
  # Schedule service execution with a certain frequency (in seconds).
  frequency_s: 20

eolica:
  input_stream: redis_input_stream_name
  output_stream: redis_output_stream_name
  # Used in case knowledge graph-based config generation is DISABLED
  config_park: path/to/park_config.yaml
  config_simulation: path/to/simulation_config.yaml
  # Used in case knowledge graph-based config generation is ENABLED
  park_name: WindparkAlkmaar
  scenario: BaselineAlkmaar

knowledge-graph:
  # Set to true to enable knowledge graph-based config generation
  enabled: true
  endpoint: http://localhost:7200/repositories/REFORMERS

redis:
  host: redis
  port: 6379
  db: 0
  password: optional_password
```

### Output Format

The simulation results published to Redis include:
- Individual turbine power production time series
- Total park power production
- Forecast timestamps
- Other simulation metadata

## Automated Model Generation

In addition to the model source code itself, a [DevOps configuration](./.gitlab-ci.yml) (for GitLab CI/CD) has been added.
This implements the automated creation of a new model generator (using the [generator manifest file](./GENERATOR-MANIFEST.yml) and [metagenerator](https://github.com/REFORMERS-EnergyValleys/reformers-dt-metagenerator)) every time a new version of the model source code is released (i.e., the code is tagged with a new version).
Together with the [knowledge graph database](https://github.com/REFORMERS-EnergyValleys/example-reformers-knowledge-graph) and the [Model API & Container Registry](https://github.com/REFORMERS-EnergyValleys/reformers-dt-model-api) prototype, this demonstrates the automated model generation workflow of the REFORMERS Digital Twin technical framework.

## Funding acknowledgement

<img alt="European Flag" src="https://upload.wikimedia.org/wikipedia/commons/thumb/b/b7/Flag_of_Europe.svg/330px-Flag_of_Europe.svg.png" align="left" style="margin-right: 10px" height="57"/> This development has been supported by the [REFORMERS] project of the European Union’s research and innovation programme Horizon Europe under the grant agreement No.101136211.

[REFORMERS]: https://reformers-energyvalleys.eu/
