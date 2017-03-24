from collections import OrderedDict
from collections.abc import MutableMapping

# Does not offer true support for java properties files, e.g. doesn't
# treat ":" as key/value delimiter, disregards escaping, etc.


class JavaProperties(MutableMapping):
    def __init__(self, properties_dict):
        super().__init__()
        self._properties_dict = properties_dict

    def __getitem__(self, name):
        return self._properties_dict[name]

    def __setitem__(self, name, value):
        self._properties_dict[name] = value

    def __delitem__(self, name):
        del self._properties_dict[name]

    def __iter__(self):
        return iter(self._properties_dict)

    def __len__(self):
        return len(self._properties_dict)

    @classmethod
    def read(cls, line_sequence):
        properties = OrderedDict()
        for line in line_sequence:
            if line.startswith("#"):
                continue
            split = line.split("=", 1)
            if len(split) == 2:
                properties[split[0]] = split[1]
        return cls(properties)

    def dump(self, file_obj):
        for key, value in self._properties_dict.items():
            file_obj.write("{}={}\n".format(key, value))
