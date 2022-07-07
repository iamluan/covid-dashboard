function main(splash, args)
    assert(splash:go(args.url))
    assert(splash:wait(2))
    for var=1, 2, 1 do
        splash.scroll_position = {y=1000000}
        assert(splash:wait(5))
    end
    return splash:html()
end