"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import zipfile
import glob
import os

def read_files(path: str) -> pd.DataFrame:
    """
    Esta función lee los archivos .zip de una carpeta y los concatena en un solo DataFrame.
    """

    df = pd.DataFrame()
    zips = glob.glob(os.path.join(path, "*"))
    for zip_name in zips:
        with zipfile.ZipFile(zip_name, 'r') as z:
            # List all files in the .zip
            zip_csv = z.namelist()[0]
            with z.open(zip_csv) as csv_file:
                df = pd.concat([df, pd.read_csv(csv_file)], ignore_index=True)
    return df                  

def generate_clients_csv(df: pd.DataFrame) -> pd.DataFrame:
    mapped = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    mapped['job'] = mapped['job'].str.replace('.', '').str.replace('-', '_')
    mapped['education'] = mapped['education'].str.replace('.', '_').replace('unknown', pd.NA)
    mapped['credit_default'] = mapped['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    mapped['mortgage'] = mapped['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
    return mapped

def generate_campaign_csv(df: pd.DataFrame) -> pd.DataFrame:
    mapped = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome']].copy()
    mapped['previous_outcome'] = mapped['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    mapped['campaign_outcome'] = mapped['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
    mapped['last_contact_date'] = pd.to_datetime(
        df['day'].astype(str) + '-' + df['month'].astype(str) + '-2022',
        format='%d-%b-%Y'
    ).astype(str)
    return mapped

def generate_economics_csv(df: pd.DataFrame) -> pd.DataFrame:
    mapped = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    return mapped

def write_file(df: pd.DataFrame, path: str):
    """
    Esta función escribe los DataFrames en archivos csv.
    """
    df.to_csv(path, index=False)

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    df = read_files('files/input/')
    clients = generate_clients_csv(df)
    campaigns = generate_campaign_csv(df)
    economics = generate_economics_csv(df)

    if os.path.exists('files/output'):
        previus_data = glob.glob(os.path.join('files/output', "*"))
        for file in previus_data:
            os.remove(file)
    else:
        os.makedirs('files/output')

    write_file(clients, 'files/output/client.csv')
    write_file(campaigns, 'files/output/campaign.csv')
    write_file(economics, 'files/output/economics.csv')    


if __name__ == "__main__":
    clean_campaign_data()
