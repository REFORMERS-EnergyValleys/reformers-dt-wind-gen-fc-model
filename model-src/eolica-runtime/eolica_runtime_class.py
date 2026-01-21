import eolica
import redis
import pathlib
import json

from .logger import Logger

class EolicaRuntime():

    def __init__(self, redis_pool: redis.ConnectionPool,
                       redis_input_stream_name: str,
                       redis_output_stream_base_name: str,
                       park_config_file: str,
                       turbine_types_config_file: str = None,
                       simulation_config_file: str = None) -> None:

        self._redis_pool = redis_pool
        self._redis_input_stream_name = redis_input_stream_name
        self._redis_output_stream_base_name = redis_output_stream_base_name

        self._park_config_file = park_config_file
        self._simulation_config_file = simulation_config_file
        self._turbine_types_config_file = turbine_types_config_file
        self.initialize_eolica()

        Logger.debug('|---- EOLICA RUNTIME ----| eolica runtime service initialized')


    def initialize_eolica(self):
        '''
        make the eolica objects, i.e. park and simulation object
        '''
        ## make the eolica park from the config that is saved
        ## if turbine_types_config_file is provided, pass it as second argument
        if self._turbine_types_config_file:
            self.park = eolica.EolicaPark(self._park_config_file, self._turbine_types_config_file)
        else:
            self.park = eolica.EolicaPark(self._park_config_file)

        ## make the simulation from the proper config
        self.simulation = eolica.EolicaSimulation(self.park, config_fn=self._simulation_config_file)


    async def run(self) -> None:
        '''
        this is the function that is called by the scheduler
        '''
        Logger.debug('|---- EOLICA RUNTIME ----| running the eolica runtime service')

        dataset = self.get_forecast()

        Logger.debug(f'|---- EOLICA RUNTIME ----| got the forecast, here is the dataset: {dataset}')

        sim_res = self.simulation.simulate_wind_timeseries(dataset)

        Logger.debug(f'|---- EOLICA RUNTIME ----| simulated the forecast, here is the result: {sim_res}')

        self.publish_forecast(sim_res)



    def publish_forecast(self, res) -> None:
        '''
        this puts the wind forecast into redis
        '''

        ## not sure this is necessary, makes strings out of ever value in the dict
        # for k,v in res.items():
        #     if k == "forecast_time":
        #         v = [item.replace(" ", "T") for item in v]
        #     res[k] = str(v)

        #print(f'==================== these are the keys of res:{res.keys()}')
        res.pop('total_production')
        res = {key: json.dumps(val) for key, val in res.items()}

        ## publish it in the redis stream
        with redis.StrictRedis(connection_pool=self._redis_pool) as r:
            r.xadd(self._redis_output_stream_base_name, res) #mapping=sim_res)

        Logger.debug(f'|---- EOLICA RUNTIME ----| published the latest forecast')


    def get_forecast(self) -> dict:
        '''
        this gets the last redis message. hopefully
        '''

        with redis.StrictRedis(connection_pool=self._redis_pool) as r:
            message_raw = r.xrevrange(self._redis_input_stream_name, count=1)

            Logger.debug(f'|---- EOLICA RUNTIME ----| received a forecast: {message_raw}')

        try:
            if not len(message_raw):
                Logger.debug(f'|---- EOLICA RUNTIME ----| no proper message received, this is the message: {message_raw}')
                raise ValueError(f'no proper message received!')

            Logger.debug(f'|---- EOLICA RUNTIME ----| got the message, here is the message: {message_raw[0][1]}')
            dataset = eolica.EolicaDataset(self._simulation_config_file, message=message_raw[0][1])

        except Exception as e:
            Logger.warning(f'|---- EOLICA RUNTIME ----| error retrieveing forecast: {e}')
            return

        return dataset
