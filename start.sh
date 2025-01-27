#!/bin/bash

DEFAULT_SF=1

if [[ ("$1" == "-dev") || ("$2" == "-dev") ]]; then
    # Run in non-detached mode to show logs
    if [[ ("$1" =~ ^[0-9]+$) ]]; then
      SCALE_FACTOR="$1" docker-compose up --build
    elif [[ ("$2" =~ ^[0-9]+$) ]]; then
      SCALE_FACTOR="$2" docker-compose up --build
    else
      SCALE_FACTOR="$DEFAULT_SF" docker-compose up --build
    fi
else
    # Run in detached mode
    if [[ ("$1" =~ ^[0-9]+$) ]]; then
      SCALE_FACTOR="$1" docker-compose up --build -d
    elif [[ ("$2" =~ ^[0-9]+$) ]]; then
      SCALE_FACTOR="$2" docker-compose up --build -d
    else
      SCALE_FACTOR="$DEFAULT_SF" docker-compose up --build -d
    fi

    sleep 4

    # Open the default browser based on the operating system
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        open http://localhost:5173/
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        xdg-open http://localhost:5173/
    elif [[ "$OSTYPE" == "cygwin" ]]; then
        # Cygwin (Windows)
        start http://localhost:5173/
    elif [[ "$OSTYPE" == "msys" ]]; then
        # MinGW (Windows)
        start http://localhost:5173/
    else
        echo "Unsupported OS. Please open the browser manually."
    fi
fi
