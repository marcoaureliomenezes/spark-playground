from datetime import timedelta ,datetime as dt
import json
import random
import time
from faker import Faker
import uuid
from random import randint


fake = Faker(locale='pt_BR')


class DataGenerator:

  def __init__(self, batch_interval=1000, min_batch_size=0, max_batch_size=2, freq=1):
    self.fake = Faker(locale='pt_BR')
    self.batch_interval = batch_interval
    self.min_batch_size = min_batch_size
    self.max_batch_size = max_batch_size
    self.freq = freq


  def gen_timestamp_stream(self, start_date, date_format="%Y-%m-%d %H:%M:%S.%f%z"):
    while True:
      batch_size = randint(self.min_batch_size, self.max_batch_size)
      for i in range(batch_size):
        event_timestamp_delta = randint(0, self.batch_interval)
        event_timestamp = start_date + timedelta(milliseconds=event_timestamp_delta)
        yield dt.strftime(event_timestamp, date_format)
      start_date = start_date + timedelta(milliseconds=self.batch_interval)
      time.sleep(self.freq)

  def gen_client(self):
    address = self.fake.address()
    address_local = ", ".join(address.split('\n')[:2]).replace("\n", ", ")
    address_cep = address.split('\n')[-1].split(" ")[0]
    address_country = "".join(address.split('\n')[-1].replace(" / ", " - ").split(" ")[1:])
    return {
      'id': uuid.uuid4().hex, 
      'name': fake.name(), 
      'birthdate': fake.date_of_birth(minimum_age=18, maximum_age=100).strftime("%Y-%m-%d"),
      'cpf': fake.cpf(),
      'monthly_income': fake.random_int(min=1000, max=10000),
      'address': address_local,
      'postal_code': address_cep,
      'city': address_country
    }


  def gen_api_key_log(self):
    api_keys = ["infura_1", "infura_2", "infura_3", "infura_4", "infura_5"]
    return {
      "value": f"API_request;{random.choice(api_keys)}"
    }
  
  def gen_item_sold(self):
    products = ['iPhone', 'TV', 'Watch', 'MacBook', 'iPad']
    return {
      "purchase_id": uuid.uuid4().hex,
      "client_id": uuid.uuid4().hex,
      "item_id": random.choice(products),
      "quantity": randint(1, 5),
      "timestamp": "2020-02-23T16:44:43.675+02:00"
    }
  
  def run(self, date_start,  method_name="gen_api_key_log", counter=0, limit=None):
    method = getattr(self, method_name)
    for timestamp in self.gen_timestamp_stream(date_start):
      msg = {**method(), "timestamp": timestamp}
      yield msg
      counter += 1
      if limit and counter >= limit: break
      
    
if __name__ == "__main__":
  pass