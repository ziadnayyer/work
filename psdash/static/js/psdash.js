
function escape_regexp(str) {
  return str.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
}

function replace_all(find, replace, str) {
  return str.replace(new RegExp(escape_regexp(find), 'g'), replace);
}

function scroll_down($el) {
    $el.scrollTop($el[0].scrollHeight);
}

function read_log() {
    var $el = $("#log-content");
    var mode = $el.data("mode");
    if(mode != "tail") {
        return;
    }
    var filename = $el.data("filename");

    $.get("/log/read", {"filename": filename}, function (resp) {
        // only scroll down if the scroll is already at the bottom.
        if(($el.scrollTop() + $el.innerHeight()) >= $el[0].scrollHeight) {
            $el.append(resp);
            scroll_down($el);
        } else {
            $el.append(resp);
        }
    });
}

function exit_search_mode() {
    var $el = $("#log-content");
    $el.data("mode", "tail");
    $controls = $("#logs .controls");
    $controls.find(".mode-text").text("Tail mode (Press s to search)");
    $controls.find(".status-text").hide();

    $.get("/log/read_tail", {"filename": $el.data("filename")}, function (resp) { 
        $el.text(resp);
        scroll_down($el);
        $("#search-input").val("").blur();
    });
}


$(document).ready(function() {
    if($("#log-content").length) {
        setInterval(read_log, 1000);
        var $el = $("#log-content");
        scroll_down($el);

        $("#scroll-down-btn").click(function() {
            scroll_down($el);
        })

        $("#search-form").submit(function(e) {
            e.preventDefault();

            var val = $("#search-input").val();
            if(!val) return;

            var $el = $("#log-content");
            var filename = $el.data("filename");
            var params = {
                "filename": filename,
                "text": val
            }

            $el.data("mode", "search");
            $("#logs .controls .mode-text").text("Search mode (ESC to exit)");

            $.get("/log/search", params, function (resp) {
                $("#logs .controls .status-text").hide();
                $el.find(".found-text").removeClass("found-text");

                var $status = $("#logs .controls .status-text");

                if(resp.position == -1) {
                    $status.text("EOF Reached.");
                } else {
                    resp.content = replace_all(params["text"], '<span class="found-text">' + params['text'] + '</span>', resp.content);
                    $el.html(resp.content);
                    $status.text("Position " + resp.position + " of " + resp.filesize + ".");
                }

                $status.show();
            });
        });
        
        $(document).keyup(function(e) {
            var mode = $el.data("mode");
            if(mode != "search" && e.which == 83) {
                $("#search-input").focus();
            }
            // Exit search mode if escape is pressed.
            else if(mode == "search" && e.which == 27) {
                exit_search_mode();
            }
        });

    }
});