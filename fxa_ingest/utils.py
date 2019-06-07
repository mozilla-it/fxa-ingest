import re
import user_agents
import logging
from datetime import datetime
import urllib.parse
from itertools import chain

def fxa_source_url(metrics):
    source_url = 'https://accounts.firefox.com/'
    query = {k: v for k, v in metrics.items() if k.startswith('utm_')}
    if query:
        source_url = '?'.join((source_url, urllib.parse.urlencode(query)))

    return source_url


def parse_user_agent(ua):
    user_agent = user_agents.parse(ua)
    return user_agent

def unixtime_to_ts(unix_ts):
    return datetime.utcfromtimestamp(int(unix_ts)).strftime('%Y-%m-%dT%H:%M:%SZ')

def calc_lag_seconds(ts):
    return (int(datetime.utcnow().timestamp()) - int(
        datetime.utcfromtimestamp(int(ts)).strftime("%s")))

def locale_to_lang(locale):
    lang = get_best_language(get_accept_languages(locale))
    return(lang or '')

def parseAcceptLanguage(acceptLanguage):
    # taken from: https://siongui.github.io/2012/10/11/python-parse-accept-language-in-http-request-header/
    languages = acceptLanguage.split(",")
    locale_q_pairs = []

    for language in languages:
        if language.split(";")[0] == language:
            # no q => q = 1
            locale_q_pairs.append((language.strip(), "1"))
        else:
            locale = language.split(";")[0].strip()
            try:
              q = language.split(";")[1].split("=")[1]
              locale_q_pairs.append((locale, q))
            except IndexError:
              continue

    return locale_q_pairs


def get_accept_languages(header_value):
    """
    Parse the user's Accept-Language HTTP header and return a list of languages
    """
    # adapted from bedrock: http://j.mp/1o3pWo5
    # and then completely stolen from: https://github.com/mozmeao/basket/blob/bcfa61b339a05267ac40c039b0bd6fa2f9b55da2/basket/news/utils.py
    if not header_value:
        return []
    languages = []
    pattern = re.compile(r'^([A-Za-z]{2,3})(?:-([A-Za-z]{2})(?:-[A-Za-z0-9]+)?)?$')

    header_value = header_value.replace('_', '-')

    parsed = parseAcceptLanguage(header_value)

    for lang, priority in parsed:
        m = pattern.match(lang)

        if not m:
            continue

#  we don't have access to newsletter_languages, so just pass it thru
#        lang = m.group(1).lower()
#
#        # Check if the shorter code is supported. This covers obsolete long
#        # codes like fr-FR (should match fr) or ja-JP (should match ja)
#        if m.group(2) and lang not in newsletter_languages():
#            lang += '-' + m.group(2).upper()

        if lang not in languages:
            languages.append(lang)

    return languages


def get_best_language(languages):
    """
    Return the best language for use with our newsletters. If none match, return first in list.
    @param languages: list of language codes.
    @return: a single language code
    """
    # short circuit, because we don't have newsletter_languages

    if not languages:
        return None

    return languages[0]

    # try again with 2 letter languages
    languages_2l = [lang[:2] for lang in languages]
    supported_langs = newsletter_languages()

    for lang in chain(languages, languages_2l):
        if lang in supported_langs:
            return lang

    return languages[0]
