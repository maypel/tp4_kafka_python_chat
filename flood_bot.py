#!/usr/bin/env python3

import sys
import threading
import re
import json

from kafka import KafkaProducer, KafkaConsumer