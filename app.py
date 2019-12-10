from flask import Flask, request, jsonify, render_template
import psycopg2
import settings_local as SETTINGS
from joblib import dump, load
import math
from datetime import datetime
import requests
import json

app = Flask(__name__)


@app.route('/api/flats/', methods=['GET'])
def get_flats():
    try:
        conn = psycopg2.connect(host=SETTINGS.host, dbname=SETTINGS.name, user=SETTINGS.user,
                                password=SETTINGS.password)
        cur = conn.cursor()
    except:
        print('fail connection')
        return jsonify({'result': False})

    cur.execute("select offer_id from flats where resource_id = 1 and closed = 'f';")

    return jsonify({'result': cur.fetchall()})

@app.route('/api/closing/', methods=['POST'])
def closer():
    try:
        conn = psycopg2.connect(host=SETTINGS.host, dbname=SETTINGS.name, user=SETTINGS.user,
                                password=SETTINGS.password)
        cur = conn.cursor()
    except:
        print('fail connection')
        return jsonify({'result': False})

    offers = json.loads(request.json)
    for offer in offers:
        cur.execute("select * from flats where offer_id = %s", (offer, ))
        print(cur.fetchall())
        cur.execute("update flats set closed = 't', updated_at = %s where offer_id = %s;", (datetime.now() ,offer, ))

    conn.commit()
    cur.close()

    return jsonify({'result': True})


@app.route('/api/save/', methods=['POST'])
def save():
    print(request.json)
    flat = json.loads(request.json)
    for price in flat['prices']:
        date = price[0].split(' ')[0]
        time = price[0].split(' ')[1]

        price[0] = datetime(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]),
                        int(time.split(':')[0]), int(time.split(':')[1]), int(time.split(':')[2][:2]))
    try:
        conn = psycopg2.connect(host=SETTINGS.host, dbname=SETTINGS.name, user=SETTINGS.user,
                                password=SETTINGS.password)
        cur = conn.cursor()
    except:
        print('fail connection')
        return jsonify({'result': False})

    cur.execute("select id from districts where name=%s;", (flat['district'],))
    try:
        district_id = cur.fetchone()[0]
    except:
        print('district does not exist')
        conn.close()
        return jsonify({'result': False})
    print('district_id' + str(district_id))

    metro_ids = {}
    for metro in flat['metros']:
        try:
            cur.execute("select id from metros where name=%s;", (metro,))
            metro_id = cur.fetchone()[0]
            metro_ids.update({metro: metro_id})
        except:
            # logging.info('metro' + str(metro) + 'does not exist')
            # try:
            #     metro_location = 'Москва,метро '+ metro
            #     coords_response = requests.get(
            #         f'https://geocode-maps.yandex.ru/1.x/?apikey={self.yand_api_token}&format=json&geocode={metro_location}', timeout=5).text
            #     coords = \
            #     json.loads(coords_response)['response']['GeoObjectCollection']['featureMember'][0]['GeoObject'][
            #         'Point']['pos']
            #     longitude, latitude = coords.split(' ')
            #     longitude = float(longitude)
            #     latitude = float(latitude)
            #
            #     cur.execute("""insert into metros (longitude, latitude, city_id, created_at, updated_at, metro_id, name)
            #                    values (%s, %s, %s, %s, %s, %s, %s)""", (
            #         longitude,
            #         latitude,
            #         1,
            #         datetime.now(),
            #         datetime.now(),
            #         0,
            #         metro
            #     ))
            #     print('udated', metro)
            # except:
            # logging.info('fail in updating' + str(metro))
            continue

    try:
        #coords_response = requests.get(
        #    'https://geocode-maps.yandex.ru/1.x/?apikey={}&format=json&geocode={}'.format(SETTINGS.yand_api_token,
        #                                                                                  flat["address"]),
        #    timeout=5).text
        #coords = \
        #    json.loads(coords_response)['response']['GeoObjectCollection']['featureMember'][0]['GeoObject'][
        #        'Point'][
        #        'pos']
        #longitude, latitude = coords.split(' ')
        longitude = float(flat['longitude'])
        latitude = float(flat['latitude'])
    except IndexError:
        print('bad address for yandex-api' + flat['address'])
        conn.close()
        return jsonify({'result': False})

    cur.execute("select id from buildings where address=%s or longitude=%s and latitude=%s;",
                (flat['address'], longitude, latitude))
    is_building_exist = cur.fetchone()
    if not is_building_exist:

        cur.execute(
            """insert into buildings
               (max_floor, building_type_str, built_year, flats_count, address, renovation,
                has_elevator, longitude, latitude, district_id, created_at, updated_at)
               values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", (
                flat['max_floor'],
                flat['building_type_str'],
                flat['built_year'],
                flat['flats_count'],
                flat['address'],
                flat['renovation'],
                flat['has_elevator'],
                longitude,
                latitude,
                district_id,
                datetime.now(),
                datetime.now()
            ))
        cur.execute("select id from buildings where address=%s;", (flat['address'],))
        building_id = cur.fetchone()[0]
        print('building_id' + str(building_id))
        for metro, metro_id in metro_ids.items():
            try:
                cur.execute(
                    """insert into time_metro_buildings (building_id, metro_id, time_to_metro, transport_type, created_at, updated_at)
                       values (%s, %s, %s, %s, %s, %s);""", (
                        building_id,
                        metro_id,
                        flat['metros'][metro]['time_to_metro'],
                        flat['metros'][metro]['transport_type'],
                        datetime.now(),
                        datetime.now()
                    ))
            except:
                print('some new error')
                conn.close()
                return jsonify({'result': False})
    else:
        building_id = is_building_exist[0]
        print('building already exist' + str(building_id))

    cur.execute('select * from flats where offer_id=%s', (flat['offer_id'],))
    is_offer_exist = cur.fetchone()
    if not is_offer_exist:
        cur.execute(
            """insert into flats (full_sq, kitchen_sq, life_sq, floor, is_apartment, building_id, created_at, updated_at, offer_id, closed, rooms_total, image, resource_id)
               values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                flat['full_sq'],
                flat['kitchen_sq'],
                flat['life_sq'],
                flat['floor'],
                flat['is_apartment'],
                building_id,
                datetime.now(),
                datetime.now(),
                flat['offer_id'],
                flat['closed'],
                flat['rooms_count'],
                flat['image'],
                1
            ))
        cur.execute('select id from flats where offer_id=%s;', (flat['offer_id'],))
        flat_id = cur.fetchone()[0]
        print('flat_id' + str(flat_id))
    else:
        flat_id = is_offer_exist[0]
        print('flat already exist' + str(flat_id))

        cur.execute("""update flats
                       set full_sq=%s, kitchen_sq=%s, life_sq=%s, floor=%s, is_apartment=%s, building_id=%s, updated_at=%s, closed=%s, rooms_total=%s, image=%s
                       where id=%s""", (
            flat['full_sq'],
            flat['kitchen_sq'],
            flat['life_sq'],
            flat['floor'],
            flat['is_apartment'],
            building_id,
            datetime.now(),
            flat['closed'],
            flat['rooms_count'],
            flat['image'],
            flat_id
        ))
        print('updated' + str(flat_id))

    for price_info in flat['prices']:
        cur.execute('select * from prices where changed_date=%s', (price_info[0],))
        is_price_exist = cur.fetchone()
        if not is_price_exist:
            cur.execute("""insert into prices (price, changed_date, flat_id, created_at, updated_at)
                           values (%s, %s, %s, %s, %s);""", (
                price_info[1],
                price_info[0],
                flat_id,
                datetime.now(),
                datetime.now()
            ))

    conn.commit()
    cur.close()

    return jsonify({'result': True})


if __name__ == '__main__':
    app.run()

