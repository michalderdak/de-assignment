import os

from components.io import Save


def test_output_file():
    save_component = Save(pipeline_root='lolo1', output_dir='lolo2')
    assert save_component._get_output_file(
        'craft_planet_date_time.csv') == \
        os.path.join('lolo1', 'lolo2', 'craft_planet.csv')
