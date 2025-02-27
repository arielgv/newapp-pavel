from sqlalchemy import Column, ForeignKey, Integer, String, Float, Date, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import VARCHAR

Base = declarative_base()

class Enterprise(Base):
    __tablename__ = 'enterprises'

    enterprise_id = Column(String, primary_key=True, nullable=False, comment="Unique ID for each enterprise")
    gln = Column(String, nullable=True, comment="GS1 Global Location Number")
    name = Column(String, nullable=False, comment="Name of the enterprise or farm")
    type = Column(String, nullable=True, comment="Type of enterprise: farm, cooperative, exporter, roaster")
    address = Column(String, nullable=True, comment="Full address of the enterprise or farm")
    region = Column(String, nullable=True, comment="Administrative region of the enterprise or farm")
    country = Column(String, nullable=True, comment="Country of the enterprise or farm")
    latitude = Column(Float, nullable=True, comment="Latitude of the location")
    longitude = Column(Float, nullable=True, comment="Longitude of the location")
    polygon = Column(Text, nullable=True, comment="GeoJSON polygon for the area covered by the enterprise or farm")

    # Relationships
    transactions_from = relationship("Transaction", foreign_keys="Transaction.from_id", back_populates="from_enterprise")
    transactions_to = relationship("Transaction", foreign_keys="Transaction.to_id", back_populates="to_enterprise")


class Transaction(Base):
    __tablename__ = 'transactions'

    transaction_id = Column(String, primary_key=True, nullable=False, comment="Unique ID for each transaction")
    from_id = Column(String, ForeignKey('enterprises.enterprise_id'), nullable=False, comment="Sender of the transaction")
    to_id = Column(String, ForeignKey('enterprises.enterprise_id'), nullable=False, comment="Receiver of the transaction")
    transaction_date = Column(Date, nullable=False, comment="Date of the transaction")
    transaction_type = Column(String, nullable=False, comment="Type of transaction: sell, transfer, processing, distribution")
    quantity = Column(Float, nullable=True, comment="Quantity involved in the transaction")
    unit = Column(String, nullable=True, comment="Unit of measurement: kg, lots")
    lot_type = Column(String, nullable=True, comment="Type of lot: raw, processed, roasted")
    description = Column(String, nullable=True, comment="Description of the lot")
    status = Column(String, nullable=True, comment="Current status of the transaction: harvested, processed, roasted, distributed")
    previous_transaction_id = Column(String, ForeignKey('transactions.transaction_id'), nullable=True, comment="Link to the preceding transaction")
    next_transaction_id = Column(String, ForeignKey('transactions.transaction_id'), nullable=True, comment="Link to the succeeding transaction")

    # Relationships
    from_enterprise = relationship("Enterprise", foreign_keys=[from_id], back_populates="transactions_from")
    to_enterprise = relationship("Enterprise", foreign_keys=[to_id], back_populates="transactions_to")
    transaction_details = relationship("TransactionDetail", back_populates="transaction")


class TransactionDetail(Base):
    __tablename__ = 'transaction_details'

    transaction_detail_id = Column(String, primary_key=True, nullable=False, comment="Unique ID for each transaction detail")
    transaction_id = Column(String, ForeignKey('transactions.transaction_id'), nullable=False, comment="Link to the transactions table")
    key = Column(String, nullable=False, comment="Name of the detail: container_number, contract_number, etc.")
    value = Column(String, nullable=False, comment="Value of the detail: ABC12345, LOT456, etc.")

    # Relationships
    transaction = relationship("Transaction", back_populates="transaction_details")


# Alembic commands can be used to generate migration scripts:
# alembic revision --autogenerate -m "Create enterprises, transactions, and transaction_details tables"
# alembic upgrade head
