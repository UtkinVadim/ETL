class ETLProcess:
    def extract(self):
        """
        Extract должен:
            - читать данные пачками;
            - спокойно переживать падение PostgreSQL;
            - начинать читать с последней обработанной записи.
        """
        pass

    def transform(self):
        """
        Transform должен:
            - обрабатывать сырые данные и преобразовывать в формат, пригодный для Elasticsearch.
        """
        pass

    def load(self):
        """
        Load должен:
            - загружать данные пачками;
            - без потерь переживать падение Elasticsearch;
            - принимать или формировать поле, которое будет считаться id в Elasticsearch.
        """
        pass
