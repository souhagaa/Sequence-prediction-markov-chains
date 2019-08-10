from flask import Blueprint
from flask_restful import Api

from UX.api.resources import UserResource, UserList, PredictionResource, LogResource


blueprint = Blueprint('api', __name__, url_prefix='')
api = Api(blueprint)


api.add_resource(UserResource, '/users/<int:user_id>')
api.add_resource(UserList, '/users')
api.add_resource(PredictionResource, '/prediction')
api.add_resource(LogResource, '/log')
