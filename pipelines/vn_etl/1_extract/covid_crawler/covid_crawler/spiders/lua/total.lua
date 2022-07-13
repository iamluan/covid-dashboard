function main(splash, args)
    assert(splash:go(args.url))
    assert(splash:wait(3))
    return splash:html()
end