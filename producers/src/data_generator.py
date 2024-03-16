from faker import Faker
import uuid
from random import randint


fake = Faker(locale='pt_BR')

def gen_data():
  return {'id': uuid.uuid4().hex, 'name': fake.name(), 'age' :randint(18,88), 'saldo': randint(0, 10000) + randint(0,100)/100}