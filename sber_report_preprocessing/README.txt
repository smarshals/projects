Функция предназначена для обработки отчетов по картам из личного кабинета в сбербанке.
    Отчеты по картам из сбербанка находятся в формате pdf.
    
    В функции используется модуль tabula для парсинга pdf файлов.
    
    Так как отчеты по кредитной карте идут немного в другом формате. Был добавлен параметр 'credit'
    Параметр "credit" (по умолчанию False)
    Для того что бы обрабатывать отчеты по кредитным картам, параметр credit нужно указать True 