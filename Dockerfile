FROM python:3

MAINTAINER Sam Nguyen <samnguyen2166@gmail.com>

ADD HelloFresh.py /

RUN pip install numpy
RUN pip install pandas
RUN pip install requests

CMD ["python", "./helloFresh.py"]
