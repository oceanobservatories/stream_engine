import logging
from preload_database.model.preload import Parameter


log = logging.getLogger(__name__)


def needs(parameter, seen=None):
    if seen is None:
        seen = set()
    needed = set()

    this_ref = PDRef(None, parameter.id)
    seen.add(this_ref)

    if parameter.parameter_type.value == 'function':
        for value in parameter.parameter_function_map.values():
            if PDRef.is_pdref(value):
                try:
                    pdref = PDRef.from_str(value)
                    needed.add(pdref)
                    if pdref in seen:
                        continue
                    seen.add(pdref)

                    sub_param = Parameter.query.get(pdref.pdid)
                    if sub_param.parameter_type.value == 'function':
                        if pdref.is_fqn():
                            log.warning('Argument error, %s, FQN only supported for L0' % pdref)
                        else:
                            needed.update(needs(sub_param, seen))

                except (ValueError, AttributeError):
                    pass

        if this_ref in needed:
            log.warning('Parameter %s defined needing itself' % this_ref)

    return needed


def needs_cc(parameter, needed=None):
    if needed is None:
        needed = []

    if parameter.parameter_type.value == 'function':
        for value in parameter.parameter_function_map.values():
            if isinstance(value,basestring) and value.startswith('CC') and value not in needed:
                needed.append(value)

    return needed


class PDRef(object):

    def __init__(self, stream_name, pdid):
        self.stream_name = stream_name
        self.pdid = pdid
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
        return str(self.chunk_key)

    def __hash__(self):
        return hash(self.chunk_key)

    def __eq__(self, other):
        return self.chunk_key == other.chunk_key
