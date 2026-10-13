package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

func imFeelingLuck(ctx context.Context, phrase string) (user, project string, err error) {
	phrase += " site:github.com"
	// try dgg
	v := url.Values{}
	v.Set("q", "! " /*I'm feeling lucky*/ +phrase)
	if user, project, err := captureRepoLocation(ctx, ("https://html.duckduckgo.com/html?" + v.Encode())); err == nil {
		return user, project, nil
	}
	// try google
	v = url.Values{}
	v.Set("btnI", "") // I'm feeling lucky
	v.Set("q", phrase)
	if user, project, err := captureRepoLocation(ctx, ("https://www.google.com/search?" + v.Encode())); err == nil {
		return user, project, nil
	}
	return "", "", errors.New("not found")
}

// uses im feeling lucky and grabs the "Location"
// header from the 302, which contains the github repo
func captureRepoLocation(ctx context.Context, url string) (user, project string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Accept", "*/*")
	// I'm a browser... :)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36")
	// roundtripper doesn't follow redirects
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return "", "", fmt.Errorf("request failed: %s", err)
	}
	resp.Body.Close()
	// assume redirection
	if resp.StatusCode/100 != 3 {
		return "", "", fmt.Errorf("non-redirect response: %d", resp.StatusCode)
	}
	// extract Location header URL
	loc := resp.Header.Get("Location")
	m := searchGithubRe.FindStringSubmatch(loc)
	if len(m) == 0 {
		return "", "", fmt.Errorf("github url not found in redirect: %s", loc)
	}
	user = m[1]
	project = m[2]
	return user, project, nil
}
