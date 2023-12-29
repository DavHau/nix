{ lib, config, ... }:
let
  inherit (lib)
    concatStrings
    mapAttrsToList
    mkOption
    types
    ;

  indent = lib.replaceStrings ["\n"] ["\n    "];

  execTestCase = testCase: ''

    ### TEST ${testCase.name}: ${testCase.description} ###

    with run_test("${testCase.description}"):
        repo = Repo("${testCase.name}")
        ${indent testCase.script}
  '';
in
{

  options = {
    setupScript = mkOption {
      type = types.lines;
      description = ''
        Python code that runs before the main test.

        Variables defined by this code will be available in the test.
      '';
      default = "";
    };
    testCases = mkOption {
      type = types.listOf (types.submodule {
        options.name = mkOption {
          type = types.str;
          description = ''
            The name of the test case.

            A repo will automatically be created in a directory with that name.
          '';
        };
        options.description = mkOption {
          type = types.str;
          description = ''
            A description of the test case.
          '';
        };
        options.script = mkOption {
          type = types.lines;
          description = ''
            Python code that runs the test.

            Variables defined by `setupScript` will be available here.
          '';
        };
      });
      description = ''
        The test cases. See `testScript`.
      '';
    };
  };

  config = {
    nodes.client = {
      environment.variables = {
        _NIX_FORCE_HTTP = "1";
      };
      nix.settings.experimental-features = ["nix-command" "flakes"];
    };
    setupScript = ''
      from contextlib import contextmanager

      class Repo:
        """
        A class to create a git repository on the gitea server and locally.
        """
        def __init__(self, name):
          self.name = name
          self.path = "/tmp/repos/" + name
          self.remote = "http://gitea:3000/test/" + name
          self.git = f"git -C {self.path}"
          self.create()

        def create(self):
          gitea.succeed(f"""
            curl --fail -X POST http://{gitea_admin}:{gitea_admin_password}@gitea:3000/api/v1/user/repos \
              -H 'Accept: application/json' -H 'Content-Type: application/json' \
              -d {shlex.quote( f'{{"name":"{self.name}", "default_branch": "main"}}' )}
          """)
          client.succeed(f"""
            mkdir -p {self.path} \
            && git init -b main {self.path} \
            && {self.git} remote add origin {self.remote}
          """)

      @contextmanager
      def run_test(description):
          """
          A context manager to run a test case.
          Prints the description when tests starts or fails.
          """
          print(f"\033[94mtesting: {description}\033[0m")
          try:
              yield
          except Exception:
              print(f"\033[91mfailed: {description}\033[0m")
              raise
    '';
    testScript = ''
      start_all();

      ${config.setupScript}

      ### SETUP COMPLETE ###

      ${lib.concatStringsSep "\n" (map execTestCase config.testCases)}
    '';
  };

}
