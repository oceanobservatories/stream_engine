import json
from engine import db


class ParameterType(db.Model):
    __tablename__ = 'parameter_type'
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String(20), nullable=False, unique=True)


class ValueEncoding(db.Model):
    __tablename__ = 'value_encoding'
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String(20), nullable=False, unique=True)


class CodeSet(db.Model):
    __tablename__ = 'code_set'
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String(250), nullable=False)


class Unit(db.Model):
    __tablename__ = 'unit'
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String(250), nullable=False, unique=True)


class FillValue(db.Model):
    __tablename__ = 'fill_value'
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String(20), nullable=False)


class FunctionType(db.Model):
    __tablename__ = 'function_type'
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String(250), nullable=False, unique=True)


class ParameterFunction(db.Model):
    __tablename__ = 'parameter_function'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(250))
    function_type_id = db.Column(db.Integer, db.ForeignKey('function_type.id'))
    function_type = db.relationship(FunctionType)
    function = db.Column(db.String(250))
    owner = db.Column(db.String(250))
    description = db.Column(db.String(4096))


class Parameter(db.Model):
    __tablename__ = 'parameter'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(250), nullable=False)
    parameter_type_id = db.Column(db.Integer, db.ForeignKey('parameter_type.id'))
    parameter_type = db.relationship(ParameterType)
    value_encoding_id = db.Column(db.Integer, db.ForeignKey('value_encoding.id'))
    value_encoding = db.relationship(ValueEncoding)
    code_set_id = db.Column(db.Integer, db.ForeignKey('code_set.id'))
    code_set = db.relationship(CodeSet)
    unit_id = db.Column(db.Integer, db.ForeignKey('unit.id'))
    unit = db.relationship(Unit)
    fill_value_id = db.Column(db.Integer, db.ForeignKey('fill_value.id'))
    fill_value = db.relationship(FillValue)
    display_name = db.Column(db.String(4096))
    precision = db.Column(db.Integer)
    parameter_function_id = db.Column(db.Integer, db.ForeignKey('parameter_function.id'))
    parameter_function = db.relationship(ParameterFunction)
    parameter_function_map = db.Column(db.PickleType(pickler=json))
    data_product_identifier = db.Column(db.String(250))
    description = db.Column(db.String(4096))
    streams = db.relationship('Stream', secondary='stream_parameter')

    def parse_pdid(self, pdid_string):
        return int(pdid_string.split()[0][2:])

    def needs(self, needed=None):
        if needed is None:
            needed = []

        if self in needed:
            return

        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():
                if value.startswith('PD'):
                    try:
                        pdid = self.parse_pdid(value)
                        sub_param = Parameter.query.get(pdid)
                        if sub_param in needed:
                            continue
                        sub_param.needs(needed)
                    except (ValueError, AttributeError):
                        pass

        if self not in needed:
            needed.append(self)
        return needed

    def needs_cc(self, needed=None):
        if needed is None:
            needed = []

        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():

                if value.startswith('CC') and value not in needed:
                    needed.append(value)

        return needed


class StreamParameter(db.Model):
    __tablename__ = 'stream_parameter'
    stream_id = db.Column(db.Integer, db.ForeignKey('stream.id'), primary_key=True)
    parameter_id = db.Column(db.Integer, db.ForeignKey('parameter.id'), primary_key=True)


class Stream(db.Model):
    __tablename__ = 'stream'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(250), nullable=False, unique=True)
    parameters = db.relationship('Parameter', secondary='stream_parameter')