#ifndef __HIREDIS_FMACRO_H
#define __HIREDIS_FMACRO_H

#if defined(__linux__)
#define _BSD_SOURCE
#define _DEFAULT_SOURCE
#endif

#if defined(__CYGWIN__)
#include <sys/cdefs.h>
#endif

#if defined(__sun__)
#define _POSIX_C_SOURCE 200112L
#else
#if !(defined(__APPLE__) && defined(__MACH__))
#define _XOPEN_SOURCE 600
#endif
#endif

#if defined(__APPLE__) && defined(__MACH__)
#define _OSX
#endif

#if defined(_MSC_VER)

#ifndef __STDC__
#define __STDC__ 1
#endif

#define INCL_WINSOCK_API_PROTOTYPES 1
#define INCL_WINSOCK_API_TYPEDEFS 0
#define WIN32_LEAN_AND_MEAN
#define _WIN32_WINNT 0x0501
#define _CRT_SECURE_NO_WARNINGS
#include <winsock2.h>
#include <ws2tcpip.h>	/* getaddrinfo() */
#include <stdio.h>

#ifndef inline
#define inline __inline
#endif

#ifndef va_copy
#define va_copy(d,s) ((d) = (s))
#endif

#ifndef snprintf
#define snprintf _snprintf
#endif

#endif

#endif
