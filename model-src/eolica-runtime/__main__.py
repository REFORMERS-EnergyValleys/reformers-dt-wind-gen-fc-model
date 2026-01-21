import argparse
import asyncio
import os
import pathlib
import pyrdp_commons.cli
import redis

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .eolica_runtime_class import EolicaRuntime
from .generate_configs_from_graphdb import generate_turbine_types_config, generate_park_config
from .knowledge_graph_adapter import KnowledgeGraphAdapter
from .logger import Logger

async def main(config: dict) -> None:
    '''
    this is the main executable in this whole rdp chain
    '''
    config_sim  = config['eolica']['config_simulation']
    config_park = config['eolica']['config_park']

    Logger.debug(f'|---- EOLICA RUNTIME ----| this is the config_sim: {config_sim}')
    Logger.debug(f'|---- EOLICA RUNTIME ----| this is the config_park: {config_park}')

    Logger.debug(f'|---- EOLICA RUNTIME ----| config_sim is an actual file: {os.path.isfile(config_sim)}')

    ## make the redic connection with specs from the config
    if 'password' in config['redis'].keys():
        pool = redis.ConnectionPool(host=config['redis']['host'],
                                    port=config['redis']['port'],
                                    db=config['redis']['db'],
                                    password=config['redis']['password'],
                                    decode_responses=True)
    else:
        pool = redis.ConnectionPool(host=config['redis']['host'],
                            port=config['redis']['port'],
                            db=config['redis']['db'],
                            decode_responses=True)

    # Extract GraphDB config if available
    graphdb_config = None
    park_name = None
    generated_turbine_types_config = None
    generated_park_config = None

    if 'graphdb' in config and config['graphdb'].get('enabled', False):
        graphdb_config = config['graphdb']
        park_name = config['eolica'].get('park_name')
        Logger.debug(f'|---- EOLICA RUNTIME ----| GraphDB enabled: {graphdb_config}')
        Logger.debug(f'|---- EOLICA RUNTIME ----| Park name: {park_name}')

        # Generate turbine types config file from GraphDB
        try:
            endpoint = graphdb_config.get('endpoint')
            if not endpoint:
                Logger.debug(f'|---- EOLICA RUNTIME ----| Warning: GraphDB endpoint not found in config, skipping turbine types config generation')
            elif not park_name:
                Logger.debug(f'|---- EOLICA RUNTIME ----| Warning: park_name not found in config, skipping turbine types config generation')
            else:
                # Get scenario from config or use default
                scenario = config.get('eolica', {}).get('scenario', 'BaselineAlkmaar')

                # Initialize adapter
                adapter = KnowledgeGraphAdapter(endpoint)

                # Determine output path: eolica-configs/turbine-types/turbinetypes_{park_name}.yaml
                config_dir = pathlib.Path(config_file_path).parent
                turbine_types_dir = config_dir / 'eolica-configs' / 'turbine-types'
                turbine_types_dir.mkdir(parents=True, exist_ok=True)

                # Create filename from park_name (convert to lowercase and replace spaces with underscores)
                filename = f"turbinetypes_{park_name.lower().replace(' ', '_')}.yaml"
                output_path = turbine_types_dir / filename

                Logger.debug(f'|---- EOLICA RUNTIME ----| Generating turbine types config from GraphDB...')
                Logger.debug(f'|---- EOLICA RUNTIME ----|   Scenario: {scenario}')
                Logger.debug(f'|---- EOLICA RUNTIME ----|   Park: {park_name}')
                Logger.debug(f'|---- EOLICA RUNTIME ----|   Output: {output_path}')

                # Generate the config file
                generate_turbine_types_config(
                    adapter=adapter,
                    scenario=scenario,
                    global_wind_atlas_site=park_name,
                    output_path=str(output_path)
                )

                Logger.debug(f'|---- EOLICA RUNTIME ----| Successfully generated turbine types config at: {output_path}')
                generated_turbine_types_config = str(output_path)

                # Generate park config file from GraphDB
                park_config_dir = config_dir / 'eolica-configs' / 'park'
                park_config_dir.mkdir(parents=True, exist_ok=True)

                # Create filename from park_name (convert to lowercase and replace spaces with underscores)
                park_filename = f"park_{park_name.lower().replace(' ', '_')}.yaml"
                park_output_path = park_config_dir / park_filename

                Logger.debug(f'|---- EOLICA RUNTIME ----| Generating park config from GraphDB...')
                Logger.debug(f'|---- EOLICA RUNTIME ----|   Scenario: {scenario}')
                Logger.debug(f'|---- EOLICA RUNTIME ----|   Park: {park_name}')
                Logger.debug(f'|---- EOLICA RUNTIME ----|   Output: {park_output_path}')

                # Generate the park config file
                generate_park_config(
                    adapter=adapter,
                    scenario=scenario,
                    global_wind_atlas_site=park_name,
                    output_path=str(park_output_path)
                )

                Logger.debug(f'|---- EOLICA RUNTIME ----| Successfully generated park config at: {park_output_path}')
                generated_park_config = str(park_output_path)
        except Exception as e:
            Logger.error(f'|---- EOLICA RUNTIME ----| Error generating configs from GraphDB: {e}')
            import traceback
            traceback.print_exc()

    Logger.debug(f'|---- EOLICA RUNTIME ----| this is the graphdb_config: {graphdb_config}')
    Logger.debug(f'|---- EOLICA RUNTIME ----| this is the park_name: {park_name} for which the graphdb is queried')

    # Use generated configs if available, otherwise use config file paths
    final_park_config = generated_park_config if generated_park_config else config_park
    final_turbine_types_config = generated_turbine_types_config if generated_turbine_types_config else None

    # client = redis.Redis(connection_pool=pool)

    eolica_simulation = EolicaRuntime(
        redis_pool=pool,
        redis_input_stream_name=config['eolica']['input_stream'],
        redis_output_stream_base_name=config['eolica']['output_stream'],
        park_config_file=final_park_config,
        turbine_types_config_file=final_turbine_types_config,
        simulation_config_file=config_sim,
    )
    Logger.debug(f'|---- EOLICA RUNTIME ----| constructed the EolicaRuntime object')

    eolica_sim_callback = eolica_simulation.run
    Logger.debug(f'|---- EOLICA RUNTIME ----| this is the eolica_sim_callback: {eolica_sim_callback}')

    ## create the asyncio event loop.
    #loop = asyncio.get_event_loop()
    #loop = asyncio.new_event_loop()
    #asyncio.set_event_loop(loop)

    # initialize the scheduler for asynchronous jobs
    scheduler = AsyncIOScheduler()
    freq_s = config['service']['frequency_s']

    try:
        ## add periodic DR events for all VENs.
        scheduler.add_job(
            eolica_sim_callback,
            'interval', seconds=freq_s
        )

        scheduler.start()

        while True:
            await asyncio.sleep(1)

        ## enter the event loop.
        #loop.run_forever()

    except KeyboardInterrupt:
        Logger.warning(f'|---- EOLICA RUNTIME ----| you hit ctrl+c, didn\'t you? ... stopping eolica')
    finally:
        Logger.debug(f'|---- EOLICA RUNTIME ----| eolica stopped')

    print(f'done')

if __name__ == '__main__':
    ## arg parser options. want to read the config
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--config', help='path to config file', default='config.yml')
    args = arg_parser.parse_args()

    ## now read the config
    config_file_path = pathlib.Path(args.config).resolve(strict=True)
    Logger.debug(f"|---- EOLICA RUNTIME ----| Configuration file for eolica-runtime found at: {config_file_path}")

    config = pyrdp_commons.cli.setup_app(config_file=str(config_file_path), env_file=None)

    asyncio.run(main(config))
