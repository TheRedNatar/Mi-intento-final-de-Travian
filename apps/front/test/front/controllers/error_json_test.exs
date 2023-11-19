defmodule Front.ErrorJSONTest do
  use Front.ConnCase, async: true

  test "renders 404" do
    assert Front.ErrorJSON.render("404.json", %{}) == %{errors: %{detail: "Not Found"}}
  end

  test "renders 500" do
    assert Front.ErrorJSON.render("500.json", %{}) ==
             %{errors: %{detail: "Internal Server Error"}}
  end
end
