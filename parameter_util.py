import logging
from preload_database.model.preload import Parameter


log = logging.getLogger(__name__)


def needs(parameter, seen=None):
    if seen is None:
        seen = set()
    needed = set()

    this_ref = PDArgument(parameter.id)
    seen.add(this_ref)

    if parameter.parameter_type.value == 'function':
        for value in parameter.parameter_function_map.values():
            converted_val = None
            if FQNArgument.is_fqn(value):
                converted_val = FQNArgument.from_preload(value)
            elif PDArgument.is_pd(value):
                converted_val = PDArgument.from_preload(value)
            elif DPIArgument.is_dpi(value):
                converted_val = DPIArgument.from_preload(value)
            elif CCArgument.is_cc(value):
                continue
            elif NumericArgument.is_num(value):
                continue
            else:
                log.error("Encountered unknown preload function argument type: %s", value)
                continue

            try:
                needed.add(converted_val)

                if converted_val in seen:
                    continue
                seen.add(converted_val)


                if isinstance(converted_val, PDArgument):
                    sub_param = Parameter.query.get(converted_val.pdid)
                    if sub_param.parameter_type.value == 'function':
                        if not isinstance(converted_val, FQNArgument):
                            sub_needs = needs(sub_param, seen)
                            needed.update(sub_needs)
                        else:
                            log.warning('Argument error, %s, FQN only supported for L0', converted_val)
            except (ValueError, AttributeError):
                pass

        if this_ref in needed:
            log.fatal('Parameter %s defined needing itself' % this_ref)

    return needed


def needs_cc(parameter, needed=None):
    if needed is None:
        needed = []

    if parameter.parameter_type.value == 'function':
        for value in parameter.parameter_function_map.values():
            if isinstance(value,basestring) and value.startswith('CC') and value not in needed:
                needed.append(value)

    return needed


class FunctionArgument(object):
    def __init__(self, stream_key):
        self.stream_key = stream_key

    @staticmethod
    def get_arg_from_val(value):
        if FQNArgument.is_fqn(value):
            converted_val = FQNArgument.from_preload(value)
        elif PDArgument.is_pd(value):
            converted_val = PDArgument.from_preload(value)
        elif DPIArgument.is_dpi(value):
            converted_val = DPIArgument.from_preload(value)
        elif CCArgument.is_cc(value):
            converted_val = CCArgument.from_preload(value)
        elif NumericArgument.is_num(value):
            converted_val = NumericArgument.from_preload(value)
        else:
            log.error("Encountered unknown preload function argument type: %s", value)
            return None

        return converted_val


class PDArgument(FunctionArgument):
    def __init__(self, pdid, stream_key=None):
        super(PDArgument, self).__init__(stream_key)
        self.pdid = int(pdid)

    @staticmethod
    def is_pd(value):
        return isinstance(value, basestring) and value.startswith("PD")

    @staticmethod
    def from_preload(value):
        if PDArgument.is_pd(value):
            pdid = value[2:] 
            return PDArgument(pdid)
        else:
            raise ValueError('%s is not a PDArgument' % value)

    def __eq__(self, other):
        if not isinstance(other, PDArgument):
            return False
        return self.pdid == other.pdid

    def __hash__(self):
        return hash(self.pdid)

    def __str__(self):
        return "PD{}".format(self.pdid)


class FQNArgument(PDArgument):
    def __init__(self, pdid, stream_name, stream_key=None):
        super(FQNArgument, self).__init__(pdid, stream_key)
        self.stream_name = str(stream_name)

    @staticmethod
    def is_fqn(value):
        return isinstance(value, basestring) and '.PD' in value

    @staticmethod
    def from_preload(value):
        if FQNArgument.is_fqn(value):
            stream, rest = value.split('.', 1)
            pdid = rest[2:]
            return FQNArgument(pdid, stream)
        else:
            raise ValueError('%s is not a FQNArgument' % value)

    def __eq__(self, other):
        if not isinstance(other, FQNArgument):
            return False
        return self.stream_name == other.stream_name and self.pdid == other.pdid

    def __hash__(self):
        return hash(self.stream_name + str(self.pdid))

    def __str__(self):
        return "fqn_{}.{}".format(self.stream_name, self.pdid)


class DPIArgument(FunctionArgument):
    def __init__(self, dpi, pdid=None, stream_key=None):
        super(DPIArgument, self).__init__(stream_key)
        self.dpi = str(dpi)
        self.pdid = pdid

    @staticmethod
    def is_dpi(value):
        return isinstance(value, basestring) and value.startswith('dpi_')

    @staticmethod
    def from_preload(value):
        if DPIArgument.is_dpi(value):
            _, dpi = value.split("_", 1)
            return DPIArgument(dpi)
        else:
            raise ValueError('%s is not a DPIArgument' % value)

    def __eq__(self, other):
        if not isinstance(other, DPIArgument):
            return False
        return self.dpi == other.dpi

    def __hash__(self):
        return hash(self.dpi)

    def __str__(self):
        return "dpi_{}".format(self.dpi)


class CCArgument(FunctionArgument):
    def __init__(self, cc):
        self.cc = cc

    @staticmethod
    def from_preload(value):
        if CCArgument.is_cc(value):
            return CCArgument(value)
        else:
            raise ValueError('%s is not a CCArgument' % value)

    def __eq__(self, other):
        if not isinstance(other, CCArgument):
            return False
        return self.cc == other.cc

    def __hash__(self):
        return hash(self.cc)

    def __str__(self):
        return self.cc
    
    @staticmethod
    def is_cc(value):
        return isinstance(value, basestring) and value.startswith("CC_")

class NumericArgument(FunctionArgument):
    def __init__(self, num):
        self.num = num

    @staticmethod
    def is_num(value):
        return isinstance(value, (int, long, float, complex))

    @staticmethod
    def from_preload(value):
        if NumericArgument.is_num(value):
            return NumericArgument(value)
        else:
            raise ValueError('%s is not a NumericArgument' % s)

    def __eq__(self, other):
        if not isinstance(other, NumericArgument):
            return False
        return self.num == other.num

    def __hash__(self):
        return hash(self.num)

    def __str__(self):
        return self.num
