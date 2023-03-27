import pandas as pd

import os
from pathlib import Path

from mobility.parsers.job_active_population import prepare_job_active_population
from mobility.parsers.permanent_db_facilities import prepare_facilities
from mobility.parsers import download_work_home_flows



def get_insee_data():
    """
    Loads the parquet files corresponding to the INSEE data
    (downloads and writes them first if needed).
    The INSEE data contains:
        - the repartition of the active population,
        - the repartion of jobs
        - the repartion of shops
        - the repartion of schools
        - the repartion of administration facilities
        - the repartion of sport facilities
        - the repartion of care facilities
        - the repartion of show facilities
        - the repartion of museum
        - the repartion of restaurants

    Returns
    -------
    dict:
        keys (list of str):
            ['jobs', 'active_population', 'malls', 'shops', 'schools',
             'admin', 'sport', 'care', 'show', 'museum', 'restaurants']
        values (list of pd.DataFrame):
            The corresponding dataframes wich have the following structure:
                Index:
                    DEPCOM (str): city geographic code
                Columns:
                    sink_volume (int): weight of the corresponding facilities
    """
    data_folder_path = Path(os.path.dirname(__file__)) / "data/insee"

    # Check if the parquet files already exist, if not writes them calling the corresponding funtion
    
    check_files = (data_folder_path / "work/jobs.parquet").exists()
    check_files = (
        check_files and (data_folder_path / "work/active_population.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/malls.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/shops.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/schools.parquet").exists()
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/admin_facilities.parquet").exists()
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/sport_facilities.parquet").exists()
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/care_facilities.parquet").exists()
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/show_facilities.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/museum.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/restaurants.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "work_home_flows/FD_MOBPRO_2019.csv").exists() # Add Home flow path
    )
    check_files =(check_files and (data_folder_path / "commune_data/donneesCommunes.csv").exists()) # Add Donneescommunes


    if not (check_files):  # ie all the files are not here
        print("Writing the INSEE (parquet/csv) files.")
        prepare_job_active_population()
        prepare_facilities()
        download_work_home_flows()

    # Load the dataframes into a dict
    insee_data = {}

    jobs = pd.read_parquet(data_folder_path / "work/jobs.parquet")
    active_population = pd.read_parquet(
        data_folder_path / "work/active_population.parquet"
    )
    shops = pd.read_parquet(data_folder_path / "facilities/shops.parquet")
    schools = pd.read_parquet(data_folder_path / "facilities/schools.parquet")
    admin = pd.read_parquet(data_folder_path / "facilities/admin_facilities.parquet")
    sport = pd.read_parquet(data_folder_path / "facilities/sport_facilities.parquet")
    care = pd.read_parquet(data_folder_path / "facilities/care_facilities.parquet")
    show = pd.read_parquet(data_folder_path / "facilities/show_facilities.parquet")
    museum = pd.read_parquet(data_folder_path / "facilities/museum.parquet")
    restaurant = pd.read_parquet(data_folder_path / "facilities/restaurants.parquet")
    raw_flowDT = pd.read_csv(data_folder_path / "work_home_flows/FD_MOBPRO_2019.csv",sep=";",
    usecols=["COMMUNE", "DCLT", "IPONDI", "TRANS"],
    dtype={"COMMUNE": str, "DCLT": str, "IPONDI": float, "TRANS": int},)
    # Attention : Ici on a "," au lien de ";"
    coordonnees=pd.read_csv(data_folder_path / "commune_data/donneesCommunes.csv",sep=",",
    usecols=['code_commune_INSEE', 'nom_commune_postal','latitude', 'longitude'],
    dtype={"code_commune_INSEE": str, "nom_commune_postal": str, "latitude": float, "longitude": float},)
    coordonnees = coordonnees.rename(columns={'code_commune_INSEE':'INSEE_COM','nom_commune_postal':'NOM_COM','latitude': 'x', 'longitude': 'y'})
    coordonnees=coordonnees.dropna(subset=['NOM_COM','x', 'y'])
    coordonnees['NOM_COM'] = coordonnees['NOM_COM'].str.replace(' ', '').str.replace('\'', '')
    new_order = ['INSEE_COM', 'NOM_COM', 'y','x']
    coordonnees = coordonnees.reindex(columns=new_order)
    #coordonnees = coordonnees.rename(columns={'y':'x','x':'y'})

    def float_to_int(val):
        return int(abs(val) * 1000)

    # apply the function to the 'x' and 'y' columns
    coordonnees['x'] = coordonnees['x'].apply(float_to_int)
    coordonnees['y'] = coordonnees['y'].apply(float_to_int)


    insee_data["jobs"] = jobs
    insee_data["active_population"] = active_population
    insee_data["shops"] = shops
    insee_data["schools"] = schools
    insee_data["admin"] = admin
    insee_data["sport"] = sport
    insee_data["care"] = care
    insee_data["show"] = show
    insee_data["museum"] = museum
    insee_data["restaurant"] = restaurant
    insee_data["raw_flowDT"]=raw_flowDT
    insee_data["coordonnees"]=coordonnees

    return insee_data
