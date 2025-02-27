from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, ForeignKey, UUID, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
from app.utils.logger import logger

# Base para los modelos de Mother Parkers (separada de la base principal)
MotherParkersBase = declarative_base()

class Entity(MotherParkersBase):
    __tablename__ = 'entity'
    entityid = Column(Integer, primary_key=True)
    entityname = Column(String)
    entitycontactfirstname = Column(String)
    entitycontactlastname = Column(String)
    entitycontactmail = Column(String)
    entitylatitude = Column(Float)
    entitylongitude = Column(Float)
    entityphonenumber = Column(String)
    entitymobilenumber = Column(String)  
    entityaddress = Column(String)
    entitycreateddate = Column(DateTime)
    entitycreateduser = Column(String)
    entitylastmodifieddate = Column(DateTime)
    entitylastmodifieduser = Column(String)
    countryid = Column(Integer)
    entitycitydistrict = Column(String)
    entitystateprovince = Column(String)
    entitygeopoint = Column(String)
    entityenabled = Column(Boolean)
    entityextid = Column(String)
    entityduplicated = Column(Boolean)
    entityzipcode = Column(String)

class Country(MotherParkersBase):
    __tablename__ = 'country'
    countryid = Column(Integer, primary_key=True)
    countryname = Column(String)
    countrycode = Column(String)
    countryalpha2 = Column(String)
    countryfullname = Column(String)

class Engagement(MotherParkersBase):
    __tablename__ = 'engagement'
    engagementid = Column(Integer, primary_key=True)
    engagementname = Column(String)
    clientid = Column(Integer)

class SaleTransaction(MotherParkersBase):
    __tablename__ = 'saletransaction'
    clientid = Column(Integer, primary_key=True)
    saletransactionid = Column(Integer, primary_key=True)
    saletransactionentityfromid = Column(Integer, ForeignKey('entity.entityid'))
    saletransactionentitytoid = Column(Integer, ForeignKey('entity.entityid'))
    engagementid = Column(Integer, ForeignKey('engagement.engagementid'))
    saletransactioncreateddate = Column(DateTime)
    saletransactioncreateduser = Column(String)
    saletransactionlastmodifieddat = Column(DateTime)
    saletransactionlastmodifieduse = Column(String)
    saletransactionparentclientid = Column(Integer)
    saletransactionparentid = Column(Integer)

class SaleTransactionParam(MotherParkersBase):
    __tablename__ = 'saletransactionparam'
    clientid = Column(Integer, primary_key=True)
    saletransactionid = Column(Integer, primary_key=True)
    cosaparamid = Column(Integer, primary_key=True)
    saletransactionparamvalue = Column(String)

class EngagementEntity(MotherParkersBase):
    __tablename__ = 'engagemententity'
    engagementid = Column(Integer, ForeignKey('engagement.engagementid'), primary_key=True)
    entityid = Column(Integer, ForeignKey('entity.entityid'), primary_key=True)

class EntityClient(MotherParkersBase):
    __tablename__ = 'entityclient'
    entityclientid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entityid = Column(Integer, ForeignKey('entity.entityid'))
    clientid = Column(Integer)

class CosaParam(MotherParkersBase):
    __tablename__ = 'cosaparam'
    cosaparamid = Column(Integer, primary_key=True)
    cosaparamsubject = Column(String(3))
    cosaparamname = Column(String)
    cosaparamultimportid = Column(Integer)