from flask import request
from flask_restful import Resource
# from flask_jwt_extended import jwt_required
from UX.utils.markov_click import predict
import os
import json
from UX.config import DATA_DIR
import traceback
import sys


class PredictionResource(Resource):
    """Single object resource
    """
    # method_decorators = [jwt_required]
    model_path = str(os.path.join(DATA_DIR, 'final/'))

    def post(self):
        current_screen = request.json.get('screen', None)
        user = request.json.get('user', None)
        if current_screen is not None and user is not None:
            try:
                user_model_path = self.model_path + "user_{}_markov_model.rds".format(user)
                print(user, user, user_model_path)
                response = predict(current_screen, user_model_path)
                return {'next_screen': response}
            except:
                traceback.print_exc(file=sys.stdout)
                return {"error": "screen not visited before by user or user not found on the server"}, 400
        else:
            return {"error": "you should provide a valid screen and user"}, 400
