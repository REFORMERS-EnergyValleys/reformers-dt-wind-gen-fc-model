import yaml
from pathlib import Path
from typing import Optional

from .knowledge_graph_adapter import KnowledgeGraphAdapter

def generate_turbine_types_config(
    adapter: KnowledgeGraphAdapter,
    scenario: str,
    global_wind_atlas_site: str,
    output_path: Optional[str] = None
) -> dict:
    """
    Generate a YAML configuration file with turbine types, power curves, and thrust curves
    by querying the graph database.

    Args:
        adapter: KnowledgeGraphAdapter instance connected to the graph database
        scenario: Scenario name to query (e.g., 'BaselineAlkmaar')
        global_wind_atlas_site: Global wind atlas site name (e.g., 'WindparkAlkmaar')
        output_path: Optional path to save the YAML file. If None, returns the dict without saving.

    Returns:
        Dictionary containing the turbine types configuration
    """
    # Retrieve turbine types from the graph database
    turbine_types_data = adapter.retrieve_turbine_types(
        scenario=scenario,
        global_wind_atlas_site=global_wind_atlas_site
    )

    # Structure the data for YAML output
    config = {
        'turbine_types': {}
    }

    # Process each turbine type
    for turbine_type_name, attributes in turbine_types_data.items():
        turbine_config = {}

        # Extract power curve if available
        power_curve_pairs = None
        power_curve_unit = None
        if 'Power Curve' in attributes:
            power_curve_data = attributes['Power Curve']
            if 'value' in power_curve_data:
                power_curve_pairs = power_curve_data['value']
            if 'unit' in power_curve_data:
                power_curve_unit = power_curve_data['unit'][1].split('/')[-1]

        # Extract thrust curve if available
        thrust_curve_pairs = None
        if 'Thrust Curve' in attributes:
            thrust_curve_data = attributes['Thrust Curve']
            if 'value' in thrust_curve_data:
                thrust_curve_pairs = thrust_curve_data['value']

        # Transform the data structure: extract binning, power_curve, and thrust_curve as separate lists
        if power_curve_pairs or thrust_curve_pairs:
            # Use power_curve pairs for binning if available, otherwise use thrust_curve pairs
            source_pairs = power_curve_pairs if power_curve_pairs else thrust_curve_pairs

            # Extract wind speeds (binning) from the first element of each pair
            turbine_config['binning'] = [pair[0] for pair in source_pairs]

            # Extract power values from power_curve pairs and convert to kW
            if power_curve_pairs:
                power_values = [pair[1] for pair in power_curve_pairs]

                # Convert to kW based on original unit
                if power_curve_unit:
                    # Handle both string and list formats
                    original_unit = power_curve_unit
                    if isinstance(power_curve_unit, list) and len(power_curve_unit) > 0:
                        original_unit = power_curve_unit[0]

                    # Convert based on unit
                    original_unit_str = str(original_unit).lower()
                    if 'megaw' in original_unit_str or 'mw' in original_unit_str:
                        # Convert from MW to kW: divide by 1e6 (as specified)
                        power_values = [val / 1e6 for val in power_values]
                    elif 'w' in original_unit_str and 'kilow' not in original_unit_str and 'kw' not in original_unit_str:
                        # Convert from W to kW: divide by 1e3
                        power_values = [val / 1e3 for val in power_values]
                    # If already kW or no unit detected, keep as is

                turbine_config['power_curve'] = power_values
                turbine_config['unit'] = 'kW'

            # Extract thrust values from thrust_curve pairs
            if thrust_curve_pairs:
                turbine_config['thrust_curve'] = [pair[1] for pair in thrust_curve_pairs]

            # Assert that all lists have the same length
            binning_len = len(turbine_config['binning'])
            if power_curve_pairs:
                power_curve_len = len(turbine_config['power_curve'])
                assert power_curve_len == binning_len, (
                    f"Length mismatch for {turbine_type_name}: "
                    f"binning has {binning_len} elements, power_curve has {power_curve_len} elements"
                )
            if thrust_curve_pairs:
                thrust_curve_len = len(turbine_config['thrust_curve'])
                assert thrust_curve_len == binning_len, (
                    f"Length mismatch for {turbine_type_name}: "
                    f"binning has {binning_len} elements, thrust_curve has {thrust_curve_len} elements"
                )

        # Only add turbine type if it has at least one curve
        if turbine_config:
            config['turbine_types'][turbine_type_name] = turbine_config

    # Save to file if output_path is provided
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Custom Dumper class to use flow style (horizontal) for lists
        class FlowListDumper(yaml.SafeDumper):
            pass

        def represent_list(dumper, data):
            return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)

        FlowListDumper.add_representer(list, represent_list)

        with open(output_file, 'w') as f:
            yaml.dump(config, f, Dumper=FlowListDumper, default_flow_style=False, sort_keys=False)

    return config


def generate_park_config(
    adapter: KnowledgeGraphAdapter,
    scenario: str,
    global_wind_atlas_site: str,
    output_path: Optional[str] = None
) -> dict:
    """
    Generate a YAML configuration file with park information and turbine details
    by querying the graph database.

    Args:
        adapter: KnowledgeGraphAdapter instance connected to the graph database
        scenario: Scenario name to query (e.g., 'BaselineAlkmaar')
        global_wind_atlas_site: Global wind atlas site name (e.g., 'WindparkAlkmaar')
        output_path: Optional path to save the YAML file. If None, returns the dict without saving.

    Returns:
        Dictionary containing the park configuration
    """
    # Retrieve windpark info (roughness)
    windpark_info = adapter.retrieve_windpark_info(
        scenario=scenario,
        global_wind_atlas_site=global_wind_atlas_site
    )

    # Retrieve turbine info
    turbine_info = adapter.retrieve_turbine_info(
        scenario=scenario,
        global_wind_atlas_site=global_wind_atlas_site
    )

    # Structure the data for YAML output
    config = {
        'park_name': global_wind_atlas_site,
        'site_type': 'GlobalWindAtlasSite',
        'roughness': None,
        'turbines': []
    }

    # Extract roughness from windpark info
    if global_wind_atlas_site in windpark_info:
        park_data = windpark_info[global_wind_atlas_site]
        if 'Roughness' in park_data and 'value' in park_data['Roughness']:
            config['roughness'] = park_data['Roughness']['value']

    # Process each turbine
    turbines_list = []
    for turbine_name, attributes in turbine_info.items():
        turbine_entry = {}

        # Extract turbine number from name (e.g., "Alkmaar 1" -> 1)
        try:
            # Try to extract number from name
            parts = turbine_name.split()
            if len(parts) > 1:
                turbine_number = int(parts[-1])
            else:
                # Fallback: use index + 1
                turbine_number = len(turbines_list) + 1
        except (ValueError, IndexError):
            turbine_number = len(turbines_list) + 1

        turbine_entry['number'] = turbine_number

        # Use turbine name as provided (first after number)
        turbine_entry['name'] = turbine_name

        # Extract turbine type and convert to lowercase with underscores
        turbine_type = None
        if 'Wind Turbine Type' in attributes and 'value' in attributes['Wind Turbine Type']:
            turbine_type = attributes['Wind Turbine Type']['value'].lower()
        turbine_entry['type'] = turbine_type

        # Extract hub height and convert to meters if needed
        if 'Hub Height' in attributes and 'value' in attributes['Hub Height']:
            hub_height_value = attributes['Hub Height']['value']
            hub_height_unit = attributes['Hub Height'].get('unit', '')

            # Convert CentiM to M (divide by 100)
            if 'CentiM' in str(hub_height_unit):
                hub_height_value = hub_height_value / 100.0

            turbine_entry['hub_height'] = hub_height_value

        # Extract rotor diameter and convert to meters if needed
        if 'Rotor Diameter' in attributes and 'value' in attributes['Rotor Diameter']:
            rotor_diameter_value = attributes['Rotor Diameter']['value']
            rotor_diameter_unit = attributes['Rotor Diameter'].get('unit', '')

            # Convert CentiM to M (divide by 100)
            if 'CentiM' in str(rotor_diameter_unit):
                rotor_diameter_value = rotor_diameter_value / 100.0

            turbine_entry['rotor_diameter'] = rotor_diameter_value

        # Extract location data (last)
        location = {}
        if 'Latitude' in attributes and 'value' in attributes['Latitude']:
            location['latitude'] = attributes['Latitude']['value']
        if 'Longitude' in attributes and 'value' in attributes['Longitude']:
            location['longitude'] = attributes['Longitude']['value']
        if 'Altitude' in attributes and 'value' in attributes['Altitude']:
            location['altitude'] = attributes['Altitude']['value']

        turbine_entry['location'] = location

        turbines_list.append(turbine_entry)

    # Sort turbines by number
    turbines_list.sort(key=lambda x: x['number'])
    config['turbines'] = turbines_list

    # Save to file if output_path is provided
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    return config


def generate_park_config_from_config_file(
    config_file_path: str,
    output_path: Optional[str] = None,
    scenario: Optional[str] = None,
    global_wind_atlas_site: Optional[str] = None
) -> dict:
    """
    Generate park config from a main config file that contains graphdb settings.

    Args:
        config_file_path: Path to the main config YAML file
        output_path: Optional path to save the generated YAML file
        scenario: Optional scenario name. If not provided, will try to infer from config
        global_wind_atlas_site: Optional park name. If not provided, will use park_name from config

    Returns:
        Dictionary containing the park configuration
    """
    import yaml as yaml_lib

    # Load the main config file
    with open(config_file_path, 'r') as f:
        config = yaml_lib.safe_load(f)

    # Extract graphdb configuration
    if 'graphdb' not in config or not config['graphdb'].get('enabled', False):
        raise ValueError("GraphDB is not enabled in the config file")

    endpoint = config['graphdb'].get('endpoint')
    if not endpoint:
        raise ValueError("GraphDB endpoint not found in config file")

    # Get scenario and park name
    if not scenario:
        # Try to infer scenario from config or use default
        scenario = config.get('eolica', {}).get('scenario', 'BaselineAlkmaar')

    if not global_wind_atlas_site:
        global_wind_atlas_site = config.get('eolica', {}).get('park_name')
        if not global_wind_atlas_site:
            raise ValueError("park_name not found in config file and global_wind_atlas_site not provided")

    # Initialize adapter
    adapter = KnowledgeGraphAdapter(endpoint)

    # Generate the config
    return generate_park_config(
        adapter=adapter,
        scenario=scenario,
        global_wind_atlas_site=global_wind_atlas_site,
        output_path=output_path
    )


def generate_turbine_types_config_from_config_file(
    config_file_path: str,
    output_path: Optional[str] = None,
    scenario: Optional[str] = None,
    global_wind_atlas_site: Optional[str] = None
) -> dict:
    """
    Generate turbine types config from a main config file that contains graphdb settings.

    Args:
        config_file_path: Path to the main config YAML file
        output_path: Optional path to save the generated YAML file
        scenario: Optional scenario name. If not provided, will try to infer from config
        global_wind_atlas_site: Optional park name. If not provided, will use park_name from config

    Returns:
        Dictionary containing the turbine types configuration
    """
    import yaml as yaml_lib

    # Load the main config file
    with open(config_file_path, 'r') as f:
        config = yaml_lib.safe_load(f)

    # Extract graphdb configuration
    if 'graphdb' not in config or not config['graphdb'].get('enabled', False):
        raise ValueError("GraphDB is not enabled in the config file")

    endpoint = config['graphdb'].get('endpoint')
    if not endpoint:
        raise ValueError("GraphDB endpoint not found in config file")

    # Get scenario and park name
    if not scenario:
        # Try to infer scenario from config or use default
        scenario = config.get('eolica', {}).get('scenario', 'BaselineAlkmaar')

    if not global_wind_atlas_site:
        global_wind_atlas_site = config.get('eolica', {}).get('park_name')
        if not global_wind_atlas_site:
            raise ValueError("park_name not found in config file and global_wind_atlas_site not provided")

    # Initialize adapter
    adapter = KnowledgeGraphAdapter(endpoint)

    # Generate the config
    return generate_turbine_types_config(
        adapter=adapter,
        scenario=scenario,
        global_wind_atlas_site=global_wind_atlas_site,
        output_path=output_path
    )
