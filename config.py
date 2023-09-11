from configparser import ConfigParser


def config(filename=r'/database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    # получить раздел, по умолчанию postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
        else:
            raise Exception("Раздел {0} не найдено в {1} файл".format(section, filename))
    return db
