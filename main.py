"""
Script ini digunakan untuk mendapatkan latitude, longitude, google maps url & image url
"""

import os
import psycopg2
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def check_type_in_response(store_type):
    """
    Function untuk mengecek tipe toko dari data yang diterima

    Args :
        response_json : data dari api

    Returns :
        response : true atau false dari pengecekan
    """
    type_options = ["health", "hospital", "pharmacy", "dentist"]
    return any(type_option in store_type for type_option in type_options)


def get_place_api(value, api_key):
    """
    Function untuk mendapatkan data api google maps

    Args :
        value : parameter yang digunakan sebagai kata kunci
        api_key : api key google maps

    Returns :
        response : respon dari api
    """
    endpoint = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

    params = {
        "input": value,
        "inputtype": "textquery",
        "fields": "formatted_address,name,place_id,types",
        "key": api_key,
    }

    if value is not None:
        response = requests.get(endpoint, params=params, timeout=10)
    else:
        response = None
    return response


def get_place_id_from_text_query(row_value, api_key):
    """
    Function untuk mendapatkan data place_id dari api google maps

    Args :
        row_value : data perbaris dari dataframe
        api_key : api key google maps

    Returns :
        place_id : ID dari lokasi per data
    """
    place_id = None

    max_retries = 3
    retry_count = 0

    while place_id is None and retry_count < max_retries:
        value = None

        # Menentukan value berdasarkan retry countnya
        if retry_count == 0:
            value = row_value["alamat_parameter"]
        elif retry_count == 1:
            value = row_value["name"] + ", " + row_value["city"]
        elif retry_count == 2:
            value = row_value["address"]

        # panggil fungsi untuk memanggil api dan mendapatkan responnya
        response = get_place_api(value, api_key)

        if response.status_code == 200:
            response_json = response.json()

            # Looping data berdasarkan jumlah data yang diterima dari api
            for i in range(len(response_json["candidates"])):
                # Pengecekan tipe toko dari data yang diterima dari api
                # if check_type_in_response(response_json, i):
                if check_type_in_response(response_json["candidates"][i]["types"]):
                    place_id = response_json["candidates"][i]["place_id"]
                    break

        retry_count += 1

    return place_id


def get_place_info_from_place_id(place_id, api_key):
    """
    Function untuk mendapatkan data place_id dari api google maps

    Args :
        row_value : data perbaris dari dataframe
        api_key : api key google maps

    Returns :
        place_id : ID dari lokasi per data
    """
    endpoint = "https://maps.googleapis.com/maps/api/place/details/json"

    params = {
        "place_id": place_id,
        "key": api_key,
        "fields": "url,photos,geometry",
    }

    response = requests.get(endpoint, params=params, timeout=10)
    response_json = response.json()

    return response_json


def get_image_url_from_photo_preference(photo_reference, api_key):
    """
    Function untuk mendapatkan data url image dari api google maps

    Args :
        photo_reference : data photo reference
        api_key : api key google maps

    Returns :
        image_url : Url dari gambar
    """
    endpoint = "https://maps.googleapis.com/maps/api/place/photo"

    params = {
        "maxwidth": 400,
        "photo_reference": photo_reference,
        "key": api_key,
    }

    response = requests.get(endpoint, params=params, timeout=10)
    image_url = response.url
    return image_url


def check_table_exits(connection, cursor):
    """
    Function untuk melakukan pengecekan apakah table sudah ada

    Args :
        connection : connection postgres
        cursor : cursor postgres
    """
    create_table_query = """
        CREATE TABLE IF NOT EXISTS public.list_rumah_sakit (
        id int8 NULL,
        province varchar(255) NULL,
        city varchar(255) NULL,
        name text NULL,
        address text NULL,
        map_url text NULL,
        latitude varchar(255) NULL,
        longitude varchar(255) NULL,
        image_url text NULL
    );
    """

    # Execute query
    cursor.execute(create_table_query)

    # Commit the transaction
    connection.commit()


def insert_data(data_frame, db_url):
    """
    Function untuk memasukkan data ke postgres dari data_frame

    Args :
        data_frame : dataframe dari data yang diterima
        db_url : link url untuk database

    Returns :
        image_url : Url dari gambar
    """
    table_name = "list_rumah_sakit"
    engine = create_engine(db_url)

    # Masukkan data data_frame ke database
    data_frame.to_sql(table_name, engine, if_exists="append", index=False)


def main():
    """
    Main Program
    """

    # Ambil variabel dari .env
    load_dotenv()
    api_key = os.getenv("KEY")
    host_db = os.getenv("DATABASE_HOST")
    name_db = os.getenv("DATABASE_NAME")
    user_db = os.getenv("DATABASE_USER")
    password_db = os.getenv("DATABASE_PASSWORD")

    # Ambil data excel dari file
    data_frame = pd.read_excel("list-provider.xlsx")

    data_frame["alamat_parameter"] = (
        data_frame["name"].astype(str)
        + ", "
        + data_frame["address"].astype(str)
        + ", "
        + data_frame["city"].astype(str)
    )
    data_frame["place_id"] = ""
    data_frame["map_url"] = ""
    data_frame["latitude"] = ""
    data_frame["longitude"] = ""
    data_frame["image_url"] = ""

    print("Mulai untuk mendapatkan latitude, longitude, google maps url & image url")
    for index, row in data_frame.iterrows():
        try:
            print(f"Processing address at index {index}: {row['name']}")
            # Panggil fungsi untuk mendapatkan URL gambar
            place_id = str(get_place_id_from_text_query(row, api_key))

            # Simpan URL gambar ke dalam DataFrame atau struktur data lainnya
            data_frame.at[index, "place_id"] = place_id

            # Memanggil fungsi untuk mendapatkan data detail
            place_result = get_place_info_from_place_id(place_id, api_key)
            data_frame.at[index, "map_url"] = str(place_result["result"]["url"])
            data_frame.at[index, "latitude"] = str(
                place_result["result"]["geometry"]["location"]["lat"]
            )
            data_frame.at[index, "longitude"] = str(
                place_result["result"]["geometry"]["location"]["lng"]
            )
            # Memanggil fungsi untuk mendapatkan url gambar (gambar yang diambil hanya gambar pertama saja karena keterbatasan limit api)
            image_url = get_image_url_from_photo_preference(
                photo_reference=place_result["result"]["photos"][0]["photo_reference"],
                api_key=api_key,
            )
            data_frame.at[index, "image_url"] = image_url

        except Exception as error:
            # Tangani kesalahan yang terjadi
            print(f"Error processing address at index {index}: {str(error)}")

    print("Selesai mendapatkan latitude, longitude, google maps url & image url")

    data_frame.to_excel("output_file_selesai_belum.xlsx", index=False)

    columns_to_delete = [
        "alamat_parameter",
        "place_id",
    ]

    # Hapus kolom yang tidak dipakai
    data_frame = data_frame.drop(columns=columns_to_delete)

    try:
        db_url = f"postgresql://{user_db}:{password_db}@{host_db}/{name_db}"

        # Establish a database connection
        connection = psycopg2.connect(db_url)

        # Create a cursor object
        cursor = connection.cursor()

        check_table_exits(connection, cursor)
        insert_data(data_frame, db_url)

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    # Simpan DataFrame dengan hasil URL gambar ke file Excel
    data_frame.to_excel("list-provider-end.xlsx", index=False)


if __name__ == "__main__":
    main()
