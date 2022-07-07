"""Message Bus package"""
import importlib.metadata
from .messagebus import MessageBus, MappingMessageBus

__version__ = importlib.metadata.version("igpy-messagebus")
