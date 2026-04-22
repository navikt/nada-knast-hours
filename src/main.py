from knast_configs import run_knast_configs_etl
from knast_hours import run_knast_hours_etl
from knast_last_used import run_knast_last_used_etl

if __name__ == '__main__':
     run_knast_configs_etl()
     run_knast_hours_etl()
     run_knast_last_used_etl()