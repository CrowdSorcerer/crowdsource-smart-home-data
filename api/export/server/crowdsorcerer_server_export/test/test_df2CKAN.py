import pandas as pd
from cmath import nan

from crowdsorcerer_server_export.df2CKAN import df_to_xml, df_to_csv, df_to_json

df = pd.DataFrame({'shape': ['square', 'circle', 'triangle'],
                   'degrees': [360, 360, 180],
                   'sides': [4, nan, 3]})


def test_df_to_xml():
    assert str(df_to_xml(df)) \
           == \
           '<?xml version="1.0" encoding="utf-8"?>\n' \
           '<data>\n' \
           '  <row>\n' \
           '    <index>0</index>\n' \
           '    <shape>square</shape>\n' \
           '    <degrees>360</degrees>\n' \
           '    <sides>4.0</sides>\n' \
           '  </row>\n' \
           '  <row>\n' \
           '    <index>1</index>\n' \
           '    <shape>circle</shape>\n' \
           '    <degrees>360</degrees>\n' \
           '    <sides/>\n' \
           '  </row>\n' \
           '  <row>\n' \
           '    <index>2</index>\n' \
           '    <shape>triangle</shape>\n' \
           '    <degrees>180</degrees>\n' \
           '    <sides>3.0</sides>\n' \
           '  </row>\n' \
           '</data>'


def test_df_to_csv():
    assert df_to_csv(df) \
           == \
           'shape,degrees,sides\n' \
           'square,360,4.0\n' \
           'circle,360,\n' \
           'triangle,180,3.0\n'


def test_df_to_json():
    assert str(df_to_json(df)) \
           == \
           '[{"shape":"square","degrees":360,"sides":4.0},' \
           '{"shape":"circle","degrees":360,"sides":null},' \
           '{"shape":"triangle","degrees":180,"sides":3.0}]'