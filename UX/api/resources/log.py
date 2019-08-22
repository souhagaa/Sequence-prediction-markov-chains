from flask import request
from flask_restful import Resource
# from flask_jwt_extended import jwt_required
from UX.tasks.pipeline import processing
from UX.config import DATA_DIR
import os


class LogResource(Resource):
    """Single object resource
    """
    # method_decorators = [jwt_required]

    def post(self):
        filenames = []
        for filename in request.files:
            file = request.files[filename]
            filenames.append(os.path.join(DATA_DIR, 'raw', file.filename))
            file.save(filenames[-1])
        processing.delay(str(os.path.join(DATA_DIR, 'raw/')))
        return "file uploaded"
