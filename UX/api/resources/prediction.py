from flask import request
from flask_restful import Resource
# from flask_jwt_extended import jwt_required
from UX.utils.markov_click.markov import predict
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

    def from_user_to_id(self, searched_user):
        file = "/home/souhagaa/Bureau/test/server/UX/UX/data/final/host_ids.json"
        with open(file) as f:
            data = json.load(f)
        for id, user in data.items():
            if user == searched_user:
                return id
        return None

    def post(self):
        current_url = request.json.get('url', None)
        user = request.json.get('user', None)
        if current_url is not None and user is not None:
            try:
                user_id = self.from_user_to_id(user)
                user_model_path = self.model_path + "user_{}_markov_model.rds".format(user_id)
                print(user, user_id, user_model_path)
                response = predict(current_url, user_model_path)
                return {'next_url': response}
            except:
                traceback.print_exc(file=sys.stdout)
                return {"error": "url or user not found on the server"}, 400
        else:
            return {"error": "you should provide a valid url and user"}, 400
