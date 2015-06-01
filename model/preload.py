import json
from engine import db
import logging

log = logging.getLogger(__name__)


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
    qc_flag = db.Column(db.String(32))


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

    def needs(self, seen=None):
        if seen is None:
            seen = set()
        needed = set()

        this_ref = PDRef(None, self.id)
        seen.add(this_ref)

        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():
                if PDRef.is_pdref(value):
                    try:
                        pdref = PDRef.from_str(value)
                        needed.add(pdref)
                        if pdref in seen:
                            continue
                        seen.add(pdref)

                        sub_param = Parameter.query.get(pdref.pdid)
                        if(sub_param.parameter_type.value == 'function'):
                            if pdref.is_fqn():
                                log.warning('Argument error, %s, FQN only supported for L0' % (pdref))
                            else:
                                needed.update(sub_param.needs(seen))

                    except (ValueError, AttributeError):
                        pass

            if this_ref in needed:
                log.warning('Parameter %s defined needing itself' % (this_ref))

        return needed

    def needs_cc(self, needed=None):
        if needed is None:
            needed = []

        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():

                if isinstance(value,basestring) and value.startswith('CC') and value not in needed:
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


class PDRef(object):

    def __init__(self, stream_name, pdid):
        self.stream_name = stream_name;
        self.pdid = pdid;
        self.chunk_key = '%s.PD%s' % (stream_name, pdid) if stream_name else pdid

    def is_fqn(self):
        return self.stream_name is not None

    @staticmethod
    def is_pdref(s):
        if isinstance(s,basestring) and (s.startswith('PD') or s.find('.PD') > 0):
            return True
        else:
            return False

    @staticmethod
    def from_str(s):
        if PDRef.is_pdref(s):
            s_split = s.split('.', 1)
            if len(s_split) == 2:
                stream_name = s_split[0]
                pdid = s_split[1]
            else:
                stream_name = None
                pdid = s

            pdid = int(pdid.split()[0][2:])
            return PDRef(stream_name, pdid)
        else:
            raise ValueError('%s is not a PDRef' % s)

    def __str__(self):
        return self.chunk_key

    def __hash__(self):
        return hash(self.chunk_key)

    def __eq__(self, other):
        return self.chunk_key == other.chunk_key
