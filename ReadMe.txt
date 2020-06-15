-------------download packages-----------
pip install psycopg2 #work with postgresql
pip insatall googletrans #translate comments and description
pip install emoji #work with emoji



-------fix instagram-scrapper------------
in dirictory  venv\Lib\site-packages\instagram_scraper\app.py
find this string answer = input( 'Repeated error {0}\n(A)bort, (I)gnore, (R)etry or retry (F)orever?'.format(exception_message) )
and change it to answer='R'