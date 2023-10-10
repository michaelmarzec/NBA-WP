# https://15xu0h4j6i.execute-api.us-east-2.amazonaws.com/dev

from flask import Flask, render_template
from flask import Flask, request, url_for


import boto3
from datetime import datetime
from datetime import date
from io import StringIO
import math
import numpy as np
import os
import pandas as pd
import sys
import gviz_api
import json

os.environ['aws_id'] = "AKIAUJGHR52YIFCXQEUO" # visible in this process + all children


os.environ['aws_secret'] = "dFvQlawsUPgiAI22rWHdxHUcviInwLMI7hI0iXFS" # visible in this process + all children

# aws_id='AKIAUJGHR52YIFCXQEUO'
# aws_secret='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'