FROM cjber/cuda

RUN mkdir ahah && cd ahah
COPY data/raw/* ./data/raw/
COPY ahah ./ahah
COPY dvc.yaml dvc.yaml
COPY Makefile Makefile

RUN yay -S wget --noconfirm \
    && wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba \
    && eval "$(./bin/micromamba shell hook -s posix)" \
    && source ~/.bashrc 
RUN  make env \
    && micromamba activate ahah

ENTRYPOINT ["dvc", "repro"]
